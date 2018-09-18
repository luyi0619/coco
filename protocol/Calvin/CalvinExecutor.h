//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinMessage.h"

#include <chrono>
#include <thread>

namespace scar {

template <class Workload> class CalvinExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = CalvinTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");

  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Calvin<DatabaseType>;

  using MessageType = CalvinMessage;
  using MessageFactoryType = CalvinMessageFactory;
  using MessageHandlerType = CalvinMessageHandler;

  CalvinExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                 ContextType &context,
                 std::vector<std::unique_ptr<TransactionType>> &transactions,
                 std::atomic<uint32_t> &complete_transaction_num,
                 std::atomic<uint32_t> &worker_status,
                 std::atomic<uint32_t> &n_completed_workers,
                 std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions),
        complete_transaction_num(complete_transaction_num),
        worker_status(worker_status), n_completed_workers(n_completed_workers),
        n_started_workers(n_started_workers),
        partitioner(
            coordinator_id, context.coordinator_num,
            CalvinHelper::get_replica_group_sizes(context.replica_group)),
        protocol(db, partitioner) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  ~CalvinExecutor() = default;

  void start() override {
    LOG(INFO) << "CalvinExecutor " << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "CalvinExecutor " << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::START);

      // make sure all lock manager stopped

      n_started_workers.fetch_add(1);

      while (complete_transaction_num.load() < context.batch_size) {

        process_request();
        if (transaction_queue.empty()) {
          continue;
        }

        TransactionType *transaction = transaction_queue.front();
        bool ok = transaction_queue.pop();
        DCHECK(ok);
        run_transaction(*transaction);
        complete_transaction_num.fetch_add(1);
      }

      n_completed_workers.fetch_add(1);

      // once all workers are stop, we need to process the replication
      // requests

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::STOP) {
        std::this_thread::yield();
      }

      n_completed_workers.fetch_add(1);
    }
  }

  void onExit() override {}

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();
    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

  void flush_messages() {

    for (auto i = 0u; i < messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = messages[i].release();

      out_queue.push(message);
      messages[i] = std::make_unique<Message>();
      init_message(messages[i].get(), i);
    }
  }

  void init_message(Message *message, std::size_t dest_node_id) {
    message->set_source_node_id(coordinator_id);
    message->set_dest_node_id(dest_node_id);
    message->set_worker_id(id);
  }

  void add_transaction(TransactionType *transaction) {
    transaction_queue.push(transaction);
  }

  void run_transaction(TransactionType &txn) {
    setupHandlers(txn);
    txn.execution_phase = true;
    auto result = txn.execute();
    n_network_size.fetch_add(txn.network_size);
    if (result == TransactionResult::READY_TO_COMMIT) {
      protocol.commit(txn);
      n_commit.fetch_add(1);
    } else if (result == TransactionResult::ABORT) {
      // non-active transactions
      n_commit.fetch_add(1);
    } else {
      n_abort_no_retry.fetch_add(1);
    }
  }

  void setupHandlers(TransactionType &txn) {
    txn.read_handler = [this, &txn](std::size_t table_id,
                                    std::size_t partition_id, std::size_t id,
                                    uint32_t key_offset, const void *key,
                                    void *value) {
      if (partitioner.has_master_partition(partition_id)) {
        TableType *table = this->db.find_table(table_id, partition_id);
        CalvinHelper::read(table->search(key), value, table->value_size());

        auto &active_coordinators = txn.active_coordinators;
        for (auto i = 0u; i < active_coordinators.size(); i++) {
          if (i == coordinator_id || !active_coordinators[i])
            continue;

          txn.network_size += MessageFactoryType::new_read_message(
              *messages[i], *table, id, key_offset, value);
        }
      } else {
        txn.pendingResponses++;
      }
    };
    txn.setup_process_requests_in_execution_phase();
    txn.remote_request_handler = [this]() { return this->process_request(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
  };

  std::size_t process_request() {

    std::size_t size = 0;

    while (!in_queue.empty()) {
      std::unique_ptr<Message> message(in_queue.front());
      bool ok = in_queue.pop();
      CHECK(ok);

      for (auto it = message->begin(); it != message->end(); it++) {

        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        DCHECK(type < messageHandlers.size());
        TableType *table = db.find_table(messagePiece.get_table_id(),
                                         messagePiece.get_partition_id());
        messageHandlers[type](messagePiece,
                              *messages[message->get_source_node_id()], *table,
                              transactions);
      }

      size += message->get_message_count();
      flush_messages();
    }
    return size;
  }

private:
  DatabaseType &db;
  ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::atomic<uint32_t> &complete_transaction_num, &worker_status;
  std::atomic<uint32_t> &n_completed_workers;
  std::atomic<uint32_t> &n_started_workers;
  RandomType random;
  CalvinPartitioner partitioner;
  ProtocolType protocol;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<
      std::function<void(MessagePiece, Message &, TableType &,
                         std::vector<std::unique_ptr<TransactionType>> &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;

  LockfreeQueue<TransactionType *> transaction_queue;
};
} // namespace scar