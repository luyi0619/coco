//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "core/Transaction.h"

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Transaction.h"

#include "core/Partitioner.h"
namespace scar {

namespace tpcc {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using RWKeyType = typename TransactionType::RWKeyType;
  using DatabaseType = typename TransactionType::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = typename NewOrder<RWKeyType, DatabaseType>::StorageType;
  static_assert(
      std::is_same<
          typename NewOrder<RWKeyType, DatabaseType>::StorageType,
          typename Payment<RWKeyType, DatabaseType>::StorageType>::value,
      "storage types do not match");

  Workload(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
           ContextType &context, RandomType &random, Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id), db(db),
        context(context), random(random), partitioner(partitioner) {}

  std::unique_ptr<TransactionType> nextTransaction(StorageType &storage) {

    int x = random.uniform_dist(1, 100);

    std::unique_ptr<TransactionType> p;

    if (x <= 50) {
      p = std::make_unique<NewOrder<RWKeyType, DatabaseType>>(
          coordinator_id, worker_id, db, context, random, partitioner, storage);
    } else {
      p = std::make_unique<Payment<RWKeyType, DatabaseType>>(
          coordinator_id, worker_id, db, context, random, partitioner, storage);
    }

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

} // namespace tpcc
} // namespace scar
