//
// Created by Yi Lu on 7/25/18.
//

#ifndef SCAR_YCSB_WORKLOAD_H
#define SCAR_YCSB_WORKLOAD_H

#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Transaction.h"

namespace scar {

namespace ycsb {

template <class Database> class Workload {
public:
  using DatabaseType = Database;
  using ContextType = typename Database::ContextType;
  using RandomType = typename Database::RandomType;
  using ProtocolType = typename Database::ProtocolType;
  using TransactionType = Transaction<Database>;

  Workload(DatabaseType &db, ContextType &context, RandomType &random,
           ProtocolType &protocol)
      : db(db), context(context), random(random), protocol(protocol) {}

  std::unique_ptr<TransactionType> nextTransaction() {

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<DatabaseType>>(db, context, random,
                                                        protocol);

    return p;
  }

private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  ProtocolType &protocol;
};

} // namespace ycsb
} // namespace scar

#endif // SCAR_YCSB_WORKLOAD_H
