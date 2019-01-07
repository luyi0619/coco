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
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;
  }

public:
  RandomType random;
  DatabaseType &db;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
};
} // namespace scar