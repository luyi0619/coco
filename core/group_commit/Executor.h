//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Percentile.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "core/group_commit/Helper.h"

#include <chrono>

namespace scar {
namespace group_commit {

template <class Workload, class Protocol> class Executor : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType =
      typename ProtocolType::template MessageHandlerType<TransactionType>;

  using StorageType = typename WorkloadType::StorageType;

  Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
           const ContextType &context, std::atomic<uint32_t> &worker_status,
           std::atomic<uint32_t> &n_complete_workers,
           std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(std::make_unique<HashReplicatedPartitioner<2>>(
            coordinator_id, context.coordinator_num)),
        protocol(db, *partitioner),
        workload(coordinator_id, id, db, random, *partitioner) {

    for (auto i = 0u; i < context.coordinator_num; i++) {
      sync_messages.emplace_back(std::make_unique<Message>());
      init_message(sync_messages[i].get(), i);

      async_messages.emplace_back(std::make_unique<Message>());
      init_message(async_messages[i].get(), i);
    }

    messageHandlers = MessageHandlerType::get_message_handlers();
  }

  void start() override {

    LOG(INFO) << "Executor " << id << " starts.";

    StorageType storage;
    uint64_t last_seed = 0;

    // transaction only commit in a single group

    std::queue<std::unique_ptr<TransactionType>> q;

    for (;;) {

      GCExecutorStatus status;
      do {
        status = static_cast<GCExecutorStatus>(worker_status.load());

        if (status == GCExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
          return;
        }
      } while (status != GCExecutorStatus::START);

      while (!q.empty()) {
        auto &ptr = q.front();
        auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - ptr->startTime)
                           .count();
        percentile.add(latency);
        q.pop();
      }

      std::size_t count = 0;
      n_started_workers.fetch_add(1);

      do {

        count++;

        bool retry_transaction = false;

        process_request();

        last_seed = random.get_seed();

        if (retry_transaction) {
          transaction->reset();
        } else {

          auto partition_num_per_node =
              context.partition_num / context.coordinator_num;
          auto partition_id =
              random.uniform_dist(0, partition_num_per_node - 1) *
                  context.coordinator_num +
              coordinator_id;

          transaction =
              workload.next_transaction(context, partition_id, storage);
          setupHandlers(*transaction);
        }

        auto result = transaction->execute();
        if (result == TransactionResult::READY_TO_COMMIT) {
          if (protocol.commit(*transaction, sync_messages, async_messages)) {
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

        if (count % context.batch_flush == 0) {
          flush_async_messages();
        }

        status = static_cast<GCExecutorStatus>(worker_status.load());
      } while (status != GCExecutorStatus::STOP);

      flush_async_messages();

      n_complete_workers.fetch_add(1);

      // once all workers are stop, we need to process the replication
      // requests

      while (static_cast<GCExecutorStatus>(worker_status.load()) !=
             GCExecutorStatus::CLEANUP) {
        process_request();
      }

      process_request();
      n_complete_workers.fetch_add(1);
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
                              *sync_messages[message->get_source_node_id()],
                              *table, *transaction);
      }

      size += message->get_message_count();
      flush_sync_messages();
    }
    return size;
  }

private:
  void setupHandlers(TransactionType &txn) {
    txn.readRequestHandler =
        [this](std::size_t table_id, std::size_t partition_id,
               uint32_t key_offset, const void *key, void *value,
               bool local_index_read) -> uint64_t {
      if (partitioner->has_master_partition(partition_id) || local_index_read) {
        return protocol.search(table_id, partition_id, key, value);
      } else {
        TableType *table = db.find_table(table_id, partition_id);
        auto coordinatorID = partitioner->master_coordinator(partition_id);
        MessageFactoryType::new_search_message(*sync_messages[coordinatorID],
                                               *table, key, key_offset);
        return 0;
      }
    };

    txn.remote_request_handler = [this]() { return process_request(); };
    txn.message_flusher = [this]() { flush_sync_messages(); };
  };

  void flush_sync_messages() {

    for (auto i = 0u; i < sync_messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (sync_messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = sync_messages[i].release();

      out_queue.push(message);
      sync_messages[i] = std::make_unique<Message>();
      init_message(sync_messages[i].get(), i);
    }
  }

  void flush_async_messages() {

    for (auto i = 0u; i < async_messages.size(); i++) {
      if (i == coordinator_id) {
        continue;
      }

      if (async_messages[i]->get_message_count() == 0) {
        continue;
      }

      auto message = async_messages[i].release();

      out_queue.push(message);
      async_messages[i] = std::make_unique<Message>();
      init_message(async_messages[i].get(), i);
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
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  std::unique_ptr<Partitioner> partitioner;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  Percentile<int64_t> percentile;
  std::unique_ptr<TransactionType> transaction;
  std::vector<std::unique_ptr<Message>> sync_messages, async_messages;
  std::vector<std::function<void(MessagePiece, Message &, TableType &,
                                 TransactionType &)>>
      messageHandlers;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace group_commit

} // namespace scar