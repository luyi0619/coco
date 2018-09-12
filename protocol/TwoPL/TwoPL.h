//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/TwoPL/TwoPLMessage.h"
#include "protocol/TwoPL/TwoPLTransaction.h"
#include <glog/logging.h>

namespace scar {

template <class Database> class TwoPL {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using TableType = ITable<MetaDataType>;
  using MessageType = TwoPLMessage;
  using TransactionType = TwoPLTransaction;

  using MessageFactoryType = TwoPLMessageFactory;
  using MessageHandlerType = TwoPLMessageHandler;

  static_assert(
      std::is_same<typename DatabaseType::TableType, TableType>::value,
      "The database table type is different from the one in protocol.");

  TwoPL(DatabaseType &db, Partitioner &partitioner)
      : db(db), partitioner(partitioner) {}

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {
    return true;
  }

private:
  DatabaseType &db;
  Partitioner &partitioner;
  uint64_t max_tid = 0;
};
} // namespace scar