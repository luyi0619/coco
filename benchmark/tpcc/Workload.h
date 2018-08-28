//
// Created by Yi Lu on 7/24/18.
//

#ifndef SCAR_TPCC_WORKLOAD_H
#define SCAR_TPCC_WORKLOAD_H

#include "core/Transaction.h"

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Transaction.h"

namespace scar {

namespace tpcc {

template <class Protocol> class Workload {
public:
  using ProtocolType = Protocol;
  using DatabaseType = Database<typename ProtocolType::MetaDataType>;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using TransactionType = Transaction<ProtocolType>;

  Workload(DatabaseType &db, ContextType &context, RandomType &random,
           ProtocolType &protocol)
      : db(db), context(context), random(random), protocol(protocol) {}

  std::unique_ptr<TransactionType> nextTransaction() {

    int x = random.uniform_dist(1, 100);

    std::unique_ptr<TransactionType> p;

    if (x <= 50) {
      p = std::make_unique<NewOrder<ProtocolType>>(db, context, random,
                                                   protocol);
    } else {
      p = std::make_unique<Payment<ProtocolType>>(db, context, random,
                                                  protocol);
    }

    return p;
  }

private:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  ProtocolType &protocol;
};

} // namespace tpcc
} // namespace scar

#endif // SCAR_TPCC_WORKLOAD_H
