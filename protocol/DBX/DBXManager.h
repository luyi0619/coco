//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/DBX/DBX.h"
#include "protocol/DBX/DBXExecutor.h"
#include "protocol/DBX/DBXHelper.h"
#include "protocol/DBX/DBXTransaction.h"

#include <atomic>
#include <thread>
#include <vector>

namespace scar {

template <class Workload> class DBXManager : public scar::Manager {
public:
  using base_type = scar::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = DBXTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  DBXManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
             const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      auto t0 = std::chrono::steady_clock::now();

      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      // then, each worker threads generates a transaction using the same seed.
      epoch.fetch_add(1);
      cleanup_batch();

      auto t1 = std::chrono::steady_clock::now();

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::DBX_READ);
      wait_all_workers_start();
      wait_all_workers_finish();

      auto t2 = std::chrono::steady_clock::now();

      // wait for all machines until they finish the DBX_READ phase.
      wait4_ack();

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::DBX_RESERVE);
      wait_all_workers_start();
      wait_all_workers_finish();

      auto t3 = std::chrono::steady_clock::now();

      // wait for all machines until they finish the DBX_READ phase.
      wait4_ack();

      // Allow each worker to commit transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::DBX_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();

      auto t4 = std::chrono::steady_clock::now();
      // wait for all machines until they finish the DBX_COMMIT phase.
      wait4_ack();

      LOG(INFO)
          << "t1-t0: "
          << std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                 .count()
          << " t2-t1: "
          << std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1)
                 .count()
          << " t3-t2: "
          << std::chrono::duration_cast<std::chrono::microseconds>(t3 - t2)
                 .count()
          << " t4-t3: "
          << std::chrono::duration_cast<std::chrono::microseconds>(t4 - t3)
                 .count();
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

      DCHECK(status == ExecutorStatus::DBX_READ);
      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      epoch.fetch_add(1);
      cleanup_batch();

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::DBX_READ);
      wait_all_workers_start();
      wait_all_workers_finish();

      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::DBX_RESERVE);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::DBX_RESERVE);
      wait_all_workers_start();
      wait_all_workers_finish();
      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::DBX_COMMIT);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::DBX_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      send_ack();
    }
  }

  void cleanup_batch() {
    std::size_t it = 0;
    for (auto i = 0u; i < transactions.size(); i++) {
      if (transactions[i] == nullptr) {
        break;
      }
      if (transactions[i]->abort_lock) {
        transactions[it++].swap(transactions[i]);
      }
    }
    total_abort.store(it);
  }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
};
} // namespace scar