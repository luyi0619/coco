//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/RStore/RStoreHelper.h"
#include "protocol/RStore/RStoreMessage.h"
#include "protocol/RStore/RStoreRWKey.h"
#include <glog/logging.h>

namespace scar {

template <class Database> class RStore {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using TableType = ITable<MetaDataType>;
  using RWKeyType = RStoreRWKey;
  using MessageType = RStoreMessage;

  using MessageFactoryType = RStoreMessageFactory<TableType>;
  using MessageHandlerType = RStoreMessageHandler<TableType>;

  static_assert(
      std::is_same<typename DatabaseType::TableType, TableType>::value,
      "The database table type is different from the one in protocol.");

  RStore(DatabaseType &db, Partitioner &partitioner)
      : db(db), partitioner(partitioner) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {

    TableType *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    return RStoreHelper::read(row, value, value_bytes);
  }

  template <class Transaction> void abort(Transaction &txn) {
    auto &writeSet = txn.writeSet;
    // unlock locked records
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];

      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      RStoreHelper::unlock(tid);
    }
  }

  template <class Transaction>
  bool commit(Transaction &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    static_assert(std::is_same<RWKeyType, typename decltype(
                                              txn.readSet)::value_type>::value,
                  "RWKeyType do not match.");
    static_assert(std::is_same<RWKeyType, typename decltype(
                                              txn.writeSet)::value_type>::value,
                  "RWKeyType do not match.");

    // lock write set
    lock_write_set(txn);

    // commit phase 2, read validation
    if (!validate_read_set(txn)) {
      abort(txn);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generateTid(txn);

    // write and replicate, use value replication for now
    write_and_replicate(txn, commit_tid, messages);

    return true;
  }

private:
  template <class Transaction> bool lock_write_set(Transaction &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    auto getReadKey = [&readSet](const void *key) -> RStoreRWKey * {
      for (auto &readKey : readSet) {
        if (readKey.get_key() == key) {
          return &readKey;
        }
      }
      return nullptr;
    };

    bool tidChanged = false;

    std::sort(writeSet.begin(), writeSet.end(),
              [](const RStoreRWKey &k1, const RStoreRWKey &k2) {
                return k1.get_sort_key() < k2.get_sort_key();
              });

    for (auto &writeKey : writeSet) {
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      uint64_t latestTid = RStoreHelper::lock(tid);
      auto readKeyPtr = getReadKey(key);
      // assume no blind write
      DCHECK(readKeyPtr != nullptr);
      uint64_t tidOnRead = readKeyPtr->get_tid();

      if (latestTid != tidOnRead) {
        tidChanged = true;
      }

      writeKey.set_tid(latestTid);
      writeKey.set_lock_bit();
    }

    return tidChanged;
  }

  template <class Transaction> bool validate_read_set(Transaction &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    auto isKeyInWriteSet = [&writeSet](const void *key) {
      for (auto &writeKey : writeSet) {
        if (writeKey.get_key() == key) {
          return true;
        }
      }
      return false;
    };

    for (auto &readKey : readSet) {
      bool in_write_set = isKeyInWriteSet(readKey.get_key());
      if (in_write_set)
        continue; // already validated in lock write set

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();
      uint64_t tid = table->search_metadata(key).load();

      if (RStoreHelper::remove_lock_bit(tid) != readKey.get_tid()) {
        return false;
      }

      if (RStoreHelper::is_locked(tid)) { // must be locked by others
        return false;
      }
    }
    return true;
  }

  template <class Transaction>

  uint64_t generateTid(Transaction &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    uint64_t next_tid = 0;

    /*
     *  A timestamp is a 64-bit word.
     *  The most significant bit is the lock bit.
     *  The lower 63 bits are for transaction sequence id.
     *  [  lock bit (1)  |  id (63) ]
     */

    // larger than the TID of any record read or written by the transaction

    for (std::size_t i = 0; i < readSet.size(); i++) {
      next_tid = std::max(next_tid, readSet[i].get_tid());
    }

    for (std::size_t i = 0; i < writeSet.size(); i++) {
      next_tid = std::max(next_tid, writeSet[i].get_tid());
    }

    // larger than the worker's most recent chosen TID

    next_tid = std::max(next_tid, max_tid);

    // increment

    next_tid++;

    // update worker's most recent chosen TID

    max_tid = next_tid;

    return next_tid;
  }

  template <class Transaction>
  void write_and_replicate(Transaction &txn, uint64_t commit_tid,
                           std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      // write
      table->update(key, value);

      // replicate
      for (auto k = 0u; k < partitioner.total_coordinators(); k++) {

        // k does not have this partition
        if (!partitioner.is_partition_replicated_on(partitionId, k)) {
          continue;
        }

        // already write
        if (k == txn.coordinator_id) {
          continue;
        }

        MessageFactoryType::new_replication_value_message(
            *messages[k], *table, writeKey.get_key(), writeKey.get_value(),
            commit_tid);
      }
    }
  }

private:
  DatabaseType &db;
  Partitioner &partitioner;
  uint64_t max_tid = 0;
};

} // namespace scar
