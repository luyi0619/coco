//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "core/Transaction.h"

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Transaction.h"

namespace scar {

namespace ycsb {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using RWKeyType = typename TransactionType::RWKeyType;
  using DatabaseType = typename TransactionType::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType =
      typename ReadModifyWrite<RWKeyType, DatabaseType>::StorageType;

  Workload(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
           ContextType &context, RandomType &random)
      : coordinator_id(coordinator_id), worker_id(worker_id), db(db),
        context(context), random(random) {}

  std::unique_ptr<TransactionType> nextTransaction(StorageType &storage) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<RWKeyType, DatabaseType>>(
            coordinator_id, worker_id, db, context, random, storage);

    return p;
  }

private:
  std::size_t coordinator_id;
  std::size_t worker_id;
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
};

} // namespace ycsb
} // namespace scar
