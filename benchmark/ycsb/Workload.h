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

    std::unique_ptr<TransactionType> p =
        std::make_unique<ReadModifyWrite<ProtocolType>>(db, context, random,
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
