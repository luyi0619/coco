//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinHelper.h"
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

  Calvin(DatabaseType &db, CalvinPartitioner &partitioner)
      : db(db), partitioner(partitioner) {}

  void abort(TransactionType &txn){
    // release read locks
    release_read_locks(txn);
  }

  bool commit(TransactionType &txn) {

    // write to db
    write(txn);

    // release read/write locks
    release_read_locks(txn);
    release_write_locks(txn);

    return true;
  }

  void write(TransactionType &txn) {

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      table->update(key, value);
    }
  }

  void release_read_locks(TransactionType &txn) {
    // release read locks
    auto &readSet = txn.readSet;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      if (!readKey.get_read_lock_bit()) {
        continue;
      }

      auto key = readKey.get_key();
      auto value = readKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      CalvinHelper::read_lock_release(tid);
    }

  }

  void release_write_locks(TransactionType &txn) {

    // release write lock
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!partitioner.has_master_partition(partitionId)) {
        continue;
      }

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      CalvinHelper::write_lock_release(tid);
    }
  }

private:
  DatabaseType &db;
  CalvinPartitioner &partitioner;
};
} // namespace scar