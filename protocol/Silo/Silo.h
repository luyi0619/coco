//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

#include "protocol/Silo/SiloRWKey.h"

namespace scar {

template <class Database> class Silo {
public:
  using DatabaseType = Database;
  using TableType = typename DatabaseType::TableType;
  using MetaDataType = std::atomic<uint64_t>;
  using RWKeyType = SiloRWKey;

  Silo(DatabaseType &db, std::atomic<uint64_t> &epoch) : db(db), epoch(epoch) {}

  template <class ValueType>
  void read(std::tuple<MetaDataType, ValueType> &row, ValueType &result) {
    MetaDataType &tid = std::get<0>(row);
    ValueType &value = std::get<1>(row);
    uint64_t tid_;
    do {
      do {
        tid_ = tid.load();
      } while (isLocked(tid_));
      result = value;
    } while (tid_ != tid.load());
  }

  template <class DataType, class ValueType>
  void update(std::tuple<DataType, ValueType> &row, const ValueType &v) {
    DataType &tid = std::get<0>(row);
    ValueType &value = std::get<1>(row);
    uint64_t tid_ = tid.load();
    CHECK(isLocked(tid_));
    value = v;
  }

  void abort(std::vector<SiloRWKey> &writeSet) {

    // unlock locked records

    for (auto &writeKey : writeSet) {
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = writeKey.get_key();
      std::atomic<uint64_t> &tid = table->searchMetaData(key);
      if (writeKey.is_lock_bit()) {
        unlock(tid);
      }
    }
  }

  bool commit(std::vector<SiloRWKey> &readSet, std::vector<SiloRWKey> &writeSet,
              uint64_t &commitEpoch) {

    // lock write set
    if (lockWriteSet(readSet, writeSet)) {
      abort(writeSet);
      return false;
    }

    // read epoch E
    commitEpoch = epoch.load();

    // commit phase 2, read validation
    if (!validateReadSet(readSet, writeSet)) {
      abort(writeSet);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generateTid(readSet, writeSet, commitEpoch);

    for (auto &writeKey : writeSet) {
      auto table_id = writeKey.get_table_id();
      auto partition_id = writeKey.get_partition_id();
      auto key = writeKey.get_key();
      auto value = writeKey.get_value();

      auto table = db.find_table(table_id, partition_id);
      std::atomic<uint64_t> &tid = table->searchMetaData(key);
      table->insert(key, value);
      unlock(tid, commit_tid);
    }

    return true;
  }

private:
  bool isLocked(uint64_t value) {
    return (value >> LOCK_BIT_OFFSET) & LOCK_BIT_MASK;
  }

  uint64_t lock(std::atomic<uint64_t> &a) {
    uint64_t oldValue, newValue;
    do {
      do {
        oldValue = a.load();
      } while (isLocked(oldValue));
      newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
    } while (!a.compare_exchange_weak(oldValue, newValue));
    CHECK(isLocked(oldValue) == false);
    return oldValue;
  }

  uint64_t lock(std::atomic<uint64_t> &a, bool &success) {
    uint64_t oldValue = a.load();

    if (isLocked(oldValue)) {
      success = false;
    } else {
      uint64_t newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
      success = a.compare_exchange_strong(oldValue, newValue);
    }
    return oldValue;
  }

  void unlock(std::atomic<uint64_t> &a) {
    uint64_t oldValue = a.load();
    CHECK(isLocked(oldValue));
    uint64_t newValue = removeLockBit(oldValue);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    CHECK(ok);
  }

  void unlock(std::atomic<uint64_t> &a, uint64_t newValue) {
    uint64_t oldValue = a.load();
    CHECK(isLocked(oldValue));
    CHECK(isLocked(newValue) == false);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    CHECK(ok);
  }

  uint64_t removeLockBit(uint64_t value) {
    return (~(LOCK_BIT_MASK << LOCK_BIT_OFFSET)) & value;
  }

  uint64_t getEpoch(uint64_t value) {
    return (value & (SILO_EPOCH_MASK << SILO_EPOCH_OFFSET)) >>
           SILO_EPOCH_OFFSET;
  }

  bool lockWriteSet(std::vector<SiloRWKey> &readSet,
                    std::vector<SiloRWKey> &writeSet) {
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
      std::atomic<uint64_t> &tid = table->searchMetaData(key);
      uint64_t latestTid = lock(tid);
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

  bool validateReadSet(std::vector<SiloRWKey> &readSet,
                       std::vector<SiloRWKey> &writeSet) {

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
      uint64_t tid = table->searchMetaData(key).load();

      if (removeLockBit(tid) != readKey.get_tid()) {
        return false;
      }

      if (isLocked(tid)) { // must be locked by others
        return false;
      }
    }
    return true;
  }

  uint64_t generateTid(std::vector<SiloRWKey> &readSet,
                       std::vector<SiloRWKey> &writeSet, uint64_t epoch) {

    // in the current global epoch
    uint64_t next_tid = epoch << SILO_EPOCH_OFFSET;

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

    CHECK(getEpoch(next_tid) == epoch);

    return maxTID;
  }

private:
  DatabaseType &db;
  std::atomic<uint64_t> &epoch;
  uint64_t maxTID = 0;

  static constexpr int SILO_EPOCH_OFFSET = 30;
  static constexpr uint64_t SILO_EPOCH_MASK = 0x1ffffffffull;
  static constexpr int LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t LOCK_BIT_MASK = 0x1ull;
};

} // namespace scar
