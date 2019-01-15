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
             std::vector<std::unique_ptr<Message>> &messages) {
    // nothing needs to be done
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value());
      }
    }

    return true;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};
} // namespace scar