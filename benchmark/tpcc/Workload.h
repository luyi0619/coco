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
  using DatabaseType = typename TransactionType::DatabaseType;
  using ContextType = Context;
  using RandomType = typename DatabaseType::RandomType;
  using StorageType = typename NewOrder<DatabaseType>::StorageType;
  static_assert(
      std::is_same<typename NewOrder<DatabaseType>::StorageType,
                   typename Payment<DatabaseType>::StorageType>::value,
      "storage types do not match");

  Workload(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
           RandomType &random, Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id), db(db),
        random(random), partitioner(partitioner) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    if (context.workloadType == TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<NewOrder<DatabaseType>>(
            coordinator_id, worker_id, partition_id, db, context, random,
            partitioner, storage);
      } else {
        p = std::make_unique<Payment<DatabaseType>>(
            coordinator_id, worker_id, partition_id, db, context, random,
            partitioner, storage);
      }
    } else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<DatabaseType>>(
          coordinator_id, worker_id, partition_id, db, context, random,
          partitioner, storage);
    } else {
      p = std::make_unique<Payment<DatabaseType>>(coordinator_id, worker_id,
                                                  partition_id, db, context,
                                                  random, partitioner, storage);
    }

    return p;
  }

private:
  std::size_t coordinator_id;
  std::size_t worker_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace tpcc
} // namespace scar
