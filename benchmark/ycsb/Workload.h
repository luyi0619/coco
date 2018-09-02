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

  Workload(DatabaseType &db, ContextType &context, RandomType &random)
      : db(db), context(context), random(random) {}

  std::unique_ptr<TransactionType> nextTransaction() {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<RWKeyType, DatabaseType>>(db, context,
                                                                   random);

    return p;
  }

private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
};

} // namespace ycsb
} // namespace scar
