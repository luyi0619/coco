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

  Workload(DatabaseType &db, ContextType &context, RandomType &random)
      : db(db), context(context), random(random) {}

  std::unique_ptr<TransactionType> nextTransaction() {

    int x = random.uniform_dist(1, 100);

    std::unique_ptr<TransactionType> p;

    if (x <= 50) {
      p = std::make_unique<NewOrder<RWKeyType, DatabaseType>>(db, context,
                                                              random);
    } else {
      p = std::make_unique<Payment<RWKeyType, DatabaseType>>(db, context,
                                                             random);
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
