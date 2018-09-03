//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloMessage.h"
#include "protocol/Silo/SiloRWKey.h"

namespace scar {

template <class Database> class Silo {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using TableType = ITable<MetaDataType>;
  using RWKeyType = SiloRWKey;

  using MessageFactoryType = SiloMessageFactory;
  using MessageHandlerType = SiloMessageHandler;

  static_assert(
      std::is_same<typename DatabaseType::TableType, TableType>::value,
      "The database table type is different from the one in protocol.");

  Silo(DatabaseType &db, std::atomic<uint64_t> &epoch) : db(db), epoch(epoch) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {

    TableType *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->valueNBytes();
    auto row = table->search(key);
    return SiloHelper::read(row, value, value_bytes);
  }

  template <class Transaction> void abort(Transaction &txn) {

    auto &writeSet = txn.writeSet;

    // unlock locked records

    for (auto &writeKey : writeSet) {
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      if (writeKey.get_lock_bit()) {
        SiloHelper::unlock(tid);
      }
    }
  }

  template <class Transaction> bool commit(Transaction &txn) {

    static_assert(std::is_same<RWKeyType, typename decltype(
                                              txn.readSet)::value_type>::value,
                  "RWKeyType do not match.");
    static_assert(std::is_same<RWKeyType, typename decltype(
                                              txn.writeSet)::value_type>::value,
                  "RWKeyType do not match.");

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    // lock write set
    if (lockWriteSet(txn)) {
      abort(txn);
      return false;
    }

    // read epoch E
    txn.commitEpoch = epoch.load();

    // commit phase 2, read validation
    if (!validateReadSet(txn)) {
      abort(txn);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generateTid(txn);

    // write
    for (auto &writeKey : writeSet) {
      auto table_id = writeKey.get_table_id();
      auto partition_id = writeKey.get_partition_id();
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      auto table = db.find_table(table_id, partition_id);
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      table->update(key, value);
      SiloHelper::unlock(tid, commit_tid);
    }

    return true;
  }

private:
  template <class Transaction> bool lockWriteSet(Transaction &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    auto getReadKey = [&readSet](const void *key) -> SiloRWKey * {
      for (auto &readKey : readSet) {
        if (readKey.get_key() == key) {
          return &readKey;
        }
      }
      return nullptr;
    };

    bool tidChanged = false;

    std::sort(writeSet.begin(), writeSet.end(),
              [](const SiloRWKey &k1, const SiloRWKey &k2) {
                return k1.get_sort_key() < k2.get_sort_key();
              });

    for (auto &writeKey : writeSet) {
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->search_metadata(key);
      uint64_t latestTid = SiloHelper::lock(tid);
      auto readKeyPtr = getReadKey(key);
      // assume no blind write
      CHECK(readKeyPtr != nullptr);
      uint64_t tidOnRead = readKeyPtr->get_tid();
      if (latestTid != tidOnRead) {
        tidChanged = true;
      }

      writeKey.set_tid(latestTid);
      writeKey.set_lock_bit();
    }

    return tidChanged;
  }

  template <class Transaction> bool validateReadSet(Transaction &txn) {

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

      if (SiloHelper::removeLockBit(tid) != readKey.get_tid()) {
        return false;
      }

      if (SiloHelper::isLocked(tid)) { // must be locked by others
        return false;
      }
    }
    return true;
  }

  template <class Transaction>

  uint64_t generateTid(Transaction &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    auto epoch = txn.commitEpoch;

    // in the current global epoch
    uint64_t next_tid = epoch << SiloHelper::SILO_EPOCH_OFFSET;

    /*
     *  A timestamp is a 64-bit word, 33 bits for epoch are sufficient for ~ 10
     * years. [   seq    |   epoch    |  lock bit ] [ 0 ... 29 |  30 ... 62 | 63
     * ]
     */

    // larger than the TID of any record read or written by the transaction

    for (std::size_t i = 0; i < readSet.size(); i++) {
      next_tid = std::max(next_tid, readSet[i].get_tid());
    }

    for (std::size_t i = 0; i < writeSet.size(); i++) {
      next_tid = std::max(next_tid, writeSet[i].get_tid());
    }

    // larger than the worker's most recent chosen TID

    next_tid = std::max(next_tid, maxTID);

    // increment

    next_tid++;

    // update worker's most recent chosen TID

    maxTID = next_tid;

    // generated tid must be in the same epoch

    CHECK(SiloHelper::getEpoch(next_tid) == epoch);

    return maxTID;
  }

private:
  DatabaseType &db;
  std::atomic<uint64_t> &epoch;
  uint64_t maxTID = 0;
};

} // namespace scar
