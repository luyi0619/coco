//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "core/Transaction.h"

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Transaction.h"

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

  Workload(DatabaseType &db, ContextType &context, RandomType &random)
      : db(db), context(context), random(random) {}

  std::unique_ptr<TransactionType> nextTransaction(StorageType &storage) {

    int x = random.uniform_dist(1, 100);

    std::unique_ptr<TransactionType> p;

    if (x <= 50) {
      p = std::make_unique<NewOrder<RWKeyType, DatabaseType>>(db, context,
                                                              random, storage);
    } else {
      p = std::make_unique<Payment<RWKeyType, DatabaseType>>(db, context,
                                                             random, storage);
    }

    return p;
  }

private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
};

} // namespace tpcc
} // namespace scar
