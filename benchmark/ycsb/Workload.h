//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"

namespace scar {

namespace ycsb {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = Database<typename TransactionType::MetaDataType>;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  Workload(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
           RandomType &random, Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id), db(db),
        random(random), partitioner(partitioner) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, worker_id, partition_id, db, context, random,
            partitioner, storage);

    return p;
  }

private:
  std::size_t coordinator_id;
  std::size_t worker_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace ycsb
} // namespace scar
