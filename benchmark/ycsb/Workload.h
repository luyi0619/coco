//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "core/Transaction.h"

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Transaction.h"

#include "core/Partitioner.h"

namespace scar {

namespace ycsb {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = typename TransactionType::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = typename ReadModifyWrite<DatabaseType>::StorageType;

  Workload(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
           ContextType &context, RandomType &random, Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id), db(db),
        context(context), random(random), partitioner(partitioner) {}

  std::unique_ptr<TransactionType> nextTransaction(StorageType &storage) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<DatabaseType>>(
            coordinator_id, worker_id, db, context, random, partitioner,
            storage);

    return p;
  }

private:
  std::size_t coordinator_id;
  std::size_t worker_id;
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace ycsb
} // namespace scar
