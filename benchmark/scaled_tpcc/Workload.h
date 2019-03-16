//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "benchmark/scaled_tpcc/Context.h"
#include "benchmark/scaled_tpcc/Database.h"
#include "benchmark/scaled_tpcc/Random.h"
#include "benchmark/scaled_tpcc/Storage.h"
#include "benchmark/scaled_tpcc/Transaction.h"
#include "core/Partitioner.h"

namespace scar {

namespace scaled_tpcc {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = Database;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
           Partitioner &partitioner)
      : coordinator_id(coordinator_id), db(db), random(random),
        partitioner(partitioner) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    if (context.workloadType == SCALED_TPCCWorkloadType::MIXED) {
      if (x <= 50) {
        p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner,
            storage);
      } else {
        p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, storage);
      }
    } else if (context.workloadType ==
               SCALED_TPCCWorkloadType::NEW_ORDER_ONLY) {
      p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id,
                                                  db, context, random,
                                                  partitioner, storage);
    } else {
      p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id,
                                                 db, context, random,
                                                 partitioner, storage);
    }

    return p;
  }

private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace scaled_tpcc
} // namespace scar
