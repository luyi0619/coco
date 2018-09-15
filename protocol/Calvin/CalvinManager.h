//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Calvin/CalvinPartitioner.h"

#include <thread>
#include <vector>

#include <boost/algorithm/string.hpp>

namespace scar {

/*
 * In the example (see comment at the top) of CalvinPartitioner.h,
 * coordinator 0 would be the real coordinator, it collects acks from
 * coordinator 1 and coordinator 2 in each batch of queries.
 */

template <class Workload> class CalvinManager : public scar::Manager {
public:
  using base_type = scar::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  CalvinManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db) {

    // parse replica_group_sizes and create partitioner

    std::vector<std::string> replica_group_sizes_string;
    boost::algorithm::split(replica_group_sizes_string, context.replica_group,
                            boost::is_any_of(","));
    std::vector<std::size_t> replica_group_sizes;
    for (auto i = 0u; i < replica_group_sizes_string.size(); i++) {
      replica_group_sizes.push_back(
          std::atoi(replica_group_sizes_string[i].c_str()));
    }
    partitioner = std::make_unique<CalvinPartitioner>(
        coordinator_id, context.coordinator_num, context.lock_manager_num,
        replica_group_sizes);

    // calculate master partitions

    for (auto i = 0u; i < context.partition_num; i++) {
      if (partitioner->has_master_partition(i)) {
        master_partitions.push_back(i);
      }
    }

    CHECK(master_partitions.size() > 0);

    // allocate storage and transactions.

    storages.resize(context.batch_size);

    // create a workload
    WorkloadType workload(coordinator_id, db, random, *partitioner);
    // generate transactions
    for (auto i = 0u; i < context.batch_size; i++) {
      auto partition_idx = random.uniform_dist(0, master_partitions.size() - 1);
      auto partition_id = master_partitions[partition_idx];
      transactions.push_back(
          workload.next_transaction(context, partition_id, storages[i]));
    }
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      n_completed_workers.store(0);
      n_started_workers.store(0);
      LOG(INFO) << "Seed: " << random.get_seed();
      // init transactions
      init_batch_transactions();
      signal_worker(ExecutorStatus::START);
      wait_all_lock_managers_start();
      wait_all_lock_managers_finish();
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_lock_managers_finish();
      wait4_ack();
    }

    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::START);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      LOG(INFO) << "Seed: " << random.get_seed();
      // init transactions
      init_batch_transactions();
      set_worker_status(ExecutorStatus::START);
      wait_all_lock_managers_start();
      wait_all_lock_managers_finish();
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_lock_managers_finish();
      send_ack();
    }
  }

  virtual void wait_all_lock_managers_finish() {
    std::size_t n_lock_manager = context.lock_manager_num;
    // wait for all workers to finish
    while (n_completed_workers.load() < n_lock_manager) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  virtual void wait_all_lock_managers_start() {
    std::size_t n_lock_manager = context.lock_manager_num;
    // wait for all workers to finish
    while (n_started_workers.load() < n_lock_manager) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void init_batch_transactions() {

    for (auto i = 0u; i < transactions.size(); i++) {
      transactions[i]->reset();
      // run execute to prepare read/write set
      transactions[i]->execute();
    }
  }

public:
  RandomType random;
  DatabaseType &db;
  std::unique_ptr<Partitioner> partitioner;
  std::vector<std::size_t> master_partitions;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace scar