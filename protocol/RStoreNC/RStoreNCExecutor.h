//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/RStore/RStore.h"
#include "protocol/RStoreNC/RStoreNCQueryNum.h"

#include <chrono>

namespace scar {

template <class Workload> class RStoreNCExecutor : public Worker {
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
  using MessageFactoryType = RStoreMessageFactory;
  using MessageHandlerType = RStoreMessageHandler;

  RStoreNCExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                   const ContextType &context,
                   std::atomic<uint32_t> &worker_status,
                   std::atomic<uint32_t> &n_complete_workers,
                   std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        s_partitioner(std::make_unique<RStoreNCSPartitioner>(
            coordinator_id, context.coordinator_num)),
        c_partitioner(std::make_unique<RStoreNCCPartitioner>(
            coordinator_id, context.coordinator_num)),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      messages.emplace_back(std::make_unique<Message>());
      init_message(messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    // C-Phase to S-Phase, to C-phase ...

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          // commit transaction in s_phase;
          commit_transactions();
          LOG(INFO) << "Executor " << id << " exits.";
          return;
        }
      } while (status != ExecutorStatus::C_PHASE);

      // commit transaction in s_phase;
      commit_transactions();

      // c_phase

      if (coordinator_id == 0) {

        n_started_workers.fetch_add(1);
        run_transaction(ExecutorStatus::C_PHASE);
        n_complete_workers.fetch_add(1);

      } else {
        n_started_workers.fetch_add(1);

        while (static_cast<ExecutorStatus>(worker_status.load()) !=
               ExecutorStatus::STOP) {
          process_request();
        }

        // process replication request after all workers stop.
        process_request();
        n_complete_workers.fetch_add(1);
      }

      // wait to s_phase

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::S_PHASE) {
        std::this_thread::yield();
      }

      // commit transaction in c_phase;
      commit_transactions();

      // s_phase

      if (coordinator_id == 0) {
        n_started_workers.fetch_add(1);

        while (static_cast<ExecutorStatus>(worker_status.load()) !=
               ExecutorStatus::STOP) {
          process_request();
        }

        // process replication request after all workers stop.
        process_request();
        n_complete_workers.fetch_add(1);
      } else {
        n_started_workers.fetch_add(1);

        run_transaction(ExecutorStatus::S_PHASE);

        n_complete_workers.fetch_add(1);
      }
    }
  }

  void commit_transactions() {
    while (!q.empty()) {
      auto &ptr = q.front();
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - ptr->startTime)
                         .count();
      percentile.add(latency);
      q.pop();
    }
  }

  void run_transaction(ExecutorStatus status) {

    std::size_t partition_id = 0, query_num = 0;

    Partitioner *partitioner = nullptr;

    ContextType phase_context;

    if (status == ExecutorStatus::C_PHASE) {
      CHECK(coordinator_id == 0);
      partition_id = random.uniform_dist(0, context.partition_num - 1);
      partitioner = c_partitioner.get();
      query_num = RStoreNCQueryNum<ContextType>::get_c_phase_query_num(context);
      phase_context = this->context.get_cross_partition_context();
    } else if (status == ExecutorStatus::S_PHASE) {
      partition_id = id * (context.coordinator_num - 1) + coordinator_id - 1;
      partitioner = s_partitioner.get();
      query_num = RStoreNCQueryNum<ContextType>::get_s_phase_query_num(context);
      phase_context = this->context.get_single_partition_context();
    } else {
      CHECK(false);
    }

    CHECK(partitioner->has_master_partition(partition_id));

    ProtocolType protocol(db, phase_context, *partitioner);
    WorkloadType workload(coordinator_id, db, random, *partitioner);

    StorageType storage;

    uint64_t last_seed = 0;

    std::unique_ptr<TransactionType> transaction;

    for (auto i = 0u; i < query_num; i++) {

      bool retry_transaction = false;

      do {
        process_request();
        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {
          transaction =
              workload.next_transaction(phase_context, partition_id, storage);
          setupHandlers(*transaction, protocol);
        }

        auto result = transaction->execute();
        if (result == TransactionResult::READY_TO_COMMIT) {
          bool commit = protocol.commit(*transaction, messages);
          n_network_size.fetch_add(transaction->network_size);
          if (commit) {
            n_commit.fetch_add(1);
            retry_transaction = false;
            q.push(std::move(transaction));
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
      } while (retry_transaction);

      if (i % phase_context.batch_flush == 0) {
        flush_messages();
      }
    }
    flush_messages();
  }

  void onExit() override {
    if (percentile.size() > 0) {
      LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
                << "ms (50%) " << percentile.nth(75) << "ms (75%) "
                << percentile.nth(99.9)
                << "ms (99.9%), size: " << percentile.size() * sizeof(int64_t)
                << " bytes.";
    }
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

        if (type == static_cast<uint32_t>(
                        ControlMessage::OPERATION_REPLICATION_REQUEST)) {
          ControlMessageHandler::operation_replication_request_handler(
              messagePiece, *messages[message->get_source_node_id()], db,
              false);
        } else {
          messageHandlers[type](
              messagePiece, *messages[message->get_source_node_id()], *table);
        }
      }

      size += message->get_message_count();
    }
    return size;
  }

  void setupHandlers(TransactionType &txn, ProtocolType &protocol) {
    txn.readRequestHandler =
        [&protocol](std::size_t table_id, std::size_t partition_id,
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
  const ContextType &context;
  std::unique_ptr<Partitioner> s_partitioner, c_partitioner;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  RandomType random;
  Percentile<int64_t> percentile;
  // transaction only commit in a single group
  std::queue<std::unique_ptr<TransactionType>> q;
  std::vector<std::unique_ptr<Message>> messages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace scar