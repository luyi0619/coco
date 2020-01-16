//
// Created by Yi Lu on 1/16/20.
//

#pragma once

#include "core/Partitioner.h"

namespace scar {
template <class Database> class PwvWorkload {
public:
  using DatabaseType = Database;
  using StorageType = typename DatabaseType::StorageType;
  using TransactionType = PwvTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  PwvWorkload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
              Partitioner &partitioner) {}
};
} // namespace scar
