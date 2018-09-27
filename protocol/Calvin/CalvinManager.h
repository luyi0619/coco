//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include <thread>
#include <vector>

namespace scar {

template <class Workload> class CalvinManager : public scar::Manager {
public:
  using base_type = scar::Manager;

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

  CalvinManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db),
        workload_context(context),
        partitioner(
            coordinator_id, context.coordinator_num,
            CalvinHelper::get_replica_group_sizes(context.replica_group)),
        workload(coordinator_id, db, random, partitioner) {

    storages.resize(context.batch_size);
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      signal_worker(ExecutorStatus::START);

      // LOG(INFO) << "Seed: " << random.get_seed();
      complete_transaction_num.store(0);
      prepare_read_write_set();

      wait4_ack();
      signal_worker(ExecutorStatus::START);

      complete_transaction_num.store(0);
      schedule_transactions();

      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::START);
      set_worker_status(ExecutorStatus::START);

      complete_transaction_num.store(0);
      prepare_read_write_set();

      send_ack();
      status = wait4_signal();
      DCHECK(status == ExecutorStatus::START);

      complete_transaction_num.store(0);
      schedule_transactions();

      send_ack();
    }
  }

  void add_worker(const std::shared_ptr<CalvinExecutor<WorkloadType>> &w) {
    workers.push_back(w);
  }

  void prepare_read_write_set() {

    transactions.clear();

    // generate transactions
    for (auto i = 0u; i < context.batch_size; i++) {
      auto partition_id = random.uniform_dist(0, context.partition_num - 1);
      transactions.push_back(workload.next_transaction(
          workload_context, partition_id, storages[i]));
      transactions[i]->set_id(i);
      workers[i % workers.size()]->add_transaction_to_prepare(
          transactions[i].get());
    }

    wait_transaction_complete();
  }

  void schedule_transactions() {

    // grant locks, once all locks are acquired, assign the transaction to
    // a worker thread in a round-robin manner.

    for (auto i = 0u; i < transactions.size(); i++) {
      // do not grant locks to abort no retry transaction
      if (!transactions[i]->abort_no_retry) {
        auto &readSet = transactions[i]->readSet;
        for (auto k = 0u; k < readSet.size(); k++) {
          auto &readKey = readSet[k];
          auto tableId = readKey.get_table_id();
          auto partitionId = readKey.get_partition_id();

          if (!partitioner.has_master_partition(partitionId)) {
            continue;
          }

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
      }
      workers[i % workers.size()]->add_transaction_to_execute(
          transactions[i].get());
    }

    wait_transaction_complete();
  }

  void wait_transaction_complete() {
    while (complete_transaction_num.load() < transactions.size()) {
      std::this_thread::yield();
    }
  }

public:
  RandomType random;
  DatabaseType &db;
  const ContextType &workload_context;
  CalvinPartitioner partitioner;
  WorkloadType workload;
  std::atomic<uint32_t> complete_transaction_num;
  std::vector<std::shared_ptr<CalvinExecutor<WorkloadType>>> workers;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace scar