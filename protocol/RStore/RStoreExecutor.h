//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/RStore/RStore.h"

#include <chrono>

namespace scar {

template <class Workload> class RStoreExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = RStore<DatabaseType>;

  using MessageType = RStoreMessage;
  using MessageFactoryType = RStoreMessageFactory<TableType>;
  using MessageHandlerType = RStoreMessageHandler<TableType>;

  RStoreExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                 ContextType &context, std::atomic<uint32_t> &worker_status,
                 std::atomic<uint32_t> &n_complete_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        partitioner(std::make_unique<RackDBPartitioner>(
            coordinator_id, context.coordinatorNum)),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        protocol(db, *partitioner),
        workload(coordinator_id, id, db, random, *partitioner) {

    for (auto i = 0u; i < context.coordinatorNum; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    ContextType c_context = context.get_cross_partition_context();
    ContextType s_context = context.get_single_partition_context();

    for (;;) {
      RStoreWorkerStatus status =
          static_cast<RStoreWorkerStatus>(worker_status.load());

      if (status == RStoreWorkerStatus::EXIT) {
        break;
      }

      if (status == RStoreWorkerStatus::STOP) {
        std::this_thread::yield();
        continue;
      }

      if (status == RStoreWorkerStatus::C_PHASE) {

        if (coordinator_id == 0) {

          run_transaction(c_context, status);
          n_complete_workers.fetch_add(1);

          while (static_cast<RStoreWorkerStatus>(worker_status.load()) !=
                 RStoreWorkerStatus::STOP) {
            std::this_thread::yield();
          }

        } else {

          while (static_cast<RStoreWorkerStatus>(worker_status.load()) !=
                 RStoreWorkerStatus::STOP) {
            process_request();
          }

          // process replication request after all workers stop.
          process_request();
          n_complete_workers.fetch_add(1);
        }

      } else if (status == RStoreWorkerStatus::S_PHASE) {

        // run transactions, for now, let's run 10
        run_transaction(s_context, status);

        n_complete_workers.fetch_add(1);

        // once all workers are stop, we need to process the replication
        // requests

        while (static_cast<RStoreWorkerStatus>(worker_status.load()) !=
               RStoreWorkerStatus::STOP) {
          std::this_thread::yield();
        }

        // n_complete_workers has been cleared
        process_request();
        n_complete_workers.fetch_add(1);

      } else {
        CHECK(false);
      }
    }

    LOG(INFO) << "Executor " << id << " exits.";
  }

  void run_transaction(const ContextType &context, RStoreWorkerStatus status) {

    StorageType storage;
    uint64_t last_seed = 0;

    std::size_t partition_id = 0;

    std::unique_ptr<TransactionType> transaction;

    for (int i = 0; i < 10; i++) {

      bool retry_transaction = false;

      process_request();
      last_seed = random.get_seed();

      if (retry_transaction) {
        transaction->reset();
      } else {
        transaction = workload.next_transaction(context, partition_id, storage);
        setupHandlers(*transaction);
      }

      auto result = transaction->execute();
      if (result == TransactionResult::READY_TO_COMMIT) {
        if (protocol.commit(*transaction, messages)) {
          n_commit.fetch_add(1);
          retry_transaction = false;
        } else {
          if (transaction->abort_lock) {
            n_abort_lock.fetch_add(1);
          } else {
            DCHECK(transaction->abort_read_validation);
            n_abort_read_validation.fetch_add(1);
          }
          random.set_seed(last_seed);
          retry_transaction = true;
        }
      } else {
        n_abort_no_retry.fetch_add(1);
      }
    }
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << "ms (50%) " << percentile.nth(75) << "ms (75%) "
              << percentile.nth(99.9)
              << "ms (99.9%), size: " << percentile.size() * sizeof(int64_t)
              << " bytes.";
  }

  void push_message(Message *message) override { in_queue.push(message); }

  Message *pop_message() override {
    if (out_queue.empty())
      return nullptr;

    Message *message = out_queue.front();
    bool ok = out_queue.pop();
    CHECK(ok);

    return message;
  }

private:
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
                              *messages[message->get_source_node_id()], *table);
      }

      size += message->get_message_count();
    }
    return size;
  }

  void setupHandlers(TransactionType &txn) {
    txn.readRequestHandler =
        [this](std::size_t table_id, std::size_t partition_id,
               uint32_t key_offset, const void *key, void *value,
               bool local_index_read) -> uint64_t {
      return protocol.search(table_id, partition_id, key, value);
    };
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

private:
  DatabaseType &db;
  ContextType &context;
  std::unique_ptr<Partitioner> partitioner;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  Percentile<int64_t> percentile;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace scar