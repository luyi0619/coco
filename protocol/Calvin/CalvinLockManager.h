//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Worker.h"
#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinMessage.h"
#include <glog/logging.h>
#include <thread>

namespace scar {

template <class Workload> class CalvinLockManager : public Worker {
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

  CalvinLockManager(std::size_t coordinator_id, std::size_t id,
                    std::size_t shard_id, DatabaseType &db,
                    ContextType &context,
                    std::vector<std::unique_ptr<TransactionType>> &transactions,
                    std::atomic<uint32_t> &worker_status,
                    std::atomic<uint32_t> &n_complete_workers,
                    std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), shard_id(shard_id), db(db),
        transactions(transactions),
        partitioner(
            coordinator_id, context.coordinator_num, context.lock_manager_num,
            CalvinHelper::get_replica_group_sizes(context.replica_group)),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers) {
    stop_flag.store(false);
  }

  ~CalvinLockManager() = default;

  void start() override {
    LOG(INFO) << "CalvinLockManager " << shard_id << " (worker id " << id
              << " ) started, ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
          return;
        }
      } while (status != ExecutorStatus::START);

      n_started_workers.fetch_add(1);

      // grant locks to each transaction in the pre-determined order

      schedule_transactions();

      n_complete_workers.fetch_add(1);

      // make sure all lock manager stopped

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::STOP) {
        std::this_thread::yield();
      }

      n_complete_workers.fetch_add(1);
    }
  }

  void onExit() override {}

  void push_message(Message *message) override { CHECK(false); }

  Message *pop_message() override { return nullptr; }

  void add_worker(const std::shared_ptr<CalvinExecutor<WorkloadType>> &w) {
    workers.push_back(w);
  }

  void schedule_transactions() {

    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.

    for (auto i = 0u; i < transactions.size(); i++) {
      auto &readSet = transactions[i]->readSet;
      for (auto k = 0u; k < readSet.size(); k++) {
        auto &readKey = readSet[k];
        auto tableId = readKey.get_table_id();
        auto partitionId = readKey.get_partition_id();
        auto table = db.find_table(tableId, partitionId);
        auto key = readKey.get_key();

        if (readKey.get_local_index_read_bit()) {
          continue;
        }

        std::atomic<uint64_t> &tid = table->search_metadata(key);
        if (readKey.get_write_lock_bit()) {
          CalvinHelper::write_lock(tid);
        } else if (readKey.get_read_lock_bit()) {
          CalvinHelper::read_lock(tid);
        } else {
          CHECK(false);
        }
      }
      workers[i % workers.size()]->add_transaction(transactions[i].get());
    }
  }

public:
  std::size_t shard_id;
  DatabaseType &db;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<std::shared_ptr<CalvinExecutor<WorkloadType>>> workers;
  CalvinPartitioner partitioner;
  std::atomic<bool> stop_flag;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
};

} // namespace scar