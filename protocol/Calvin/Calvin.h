//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinMessage.h"
#include "protocol/Calvin/CalvinTransaction.h"

namespace scar {

template <class Database> class Calvin {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using TableType = ITable<MetaDataType>;
  using MessageType = CalvinMessage;
  using TransactionType = CalvinTransaction;

  using MessageFactoryType = CalvinMessageFactory;
  using MessageHandlerType = CalvinMessageHandler;

  static_assert(
      std::is_same<typename DatabaseType::TableType, TableType>::value,
      "The database table type is different from the one in protocol.");

  Calvin(DatabaseType &db, Partitioner &partitioner)
      : db(db), partitioner(partitioner) {}

private:
  DatabaseType &db;
  Partitioner &partitioner;
};
} // namespace scar