//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/DBX/DBXHelper.h"
#include "protocol/DBX/DBXMessage.h"
#include "protocol/DBX/DBXTransaction.h"

namespace scar {

template <class Database> class DBX {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using TableType = ITable<MetaDataType>;
  using MessageType = DBXMessage;
  using TransactionType = DBXTransaction;

  using MessageFactoryType = DBXMessageFactory;
  using MessageHandlerType = DBXMessageHandler;

  static_assert(
      std::is_same<typename DatabaseType::TableType, TableType>::value,
      "The database table type is different from the one in protocol.");

  DBX(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {}

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    return true;
  }

  void sync_messages(TransactionType &txn) { txn.message_flusher(); }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};
} // namespace scar