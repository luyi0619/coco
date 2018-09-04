//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloMessage.h"
#include "protocol/Silo/SiloRWKey.h"
#include <glog/logging.h>

namespace scar {

template <class Database> class Silo {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using TableType = ITable<MetaDataType>;
  using RWKeyType = SiloRWKey;
  using MessageType = SiloMessage;

  template <class Table> using MessageFactoryType = SiloMessageFactory<Table>;

  template <class Table, class Transaction>
  using MessageHandlerType = SiloMessageHandler<Table, Transaction>;

  static_assert(
      std::is_same<typename DatabaseType::TableType, TableType>::value,
      "The database table type is different from the one in protocol.");

  Silo(DatabaseType &db, std::atomic<uint64_t> &epoch, Partitioner &partitioner)
      : db(db), epoch(epoch), partitioner(partitioner) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {

    TableType *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->valueNBytes();
    auto row = table->search(key);
    return SiloHelper::read(row, value, value_bytes);
  }

  template <class Transaction>
  void abort(Transaction &txn,
             std::vector<std::unique_ptr<Message>> &messages) {

    auto &writeSet = txn.writeSet;

    // unlock locked records

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      // only unlock locked records
      if (!writeKey.get_lock_bit())
        continue;
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        if (writeKey.get_lock_bit()) {
          SiloHelper::unlock(tid);
        }
      } else {
        txn.pendingResponses++;
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        MessageFactoryType<TableType>::new_abort_message(
            *messages[coordinatorID], *table, writeKey.get_key());
      }
    }

    txn.messageFlusher();
  }

  template <class Transaction>
  bool commit(Transaction &txn,
              std::vector<std::unique_ptr<Message>> &syncMessages,
              std::vector<std::unique_ptr<Message>> &asyncMessages) {

    static_assert(std::is_same<RWKeyType, typename decltype(
                                              txn.readSet)::value_type>::value,
                  "RWKeyType do not match.");
    static_assert(std::is_same<RWKeyType, typename decltype(
                                              txn.writeSet)::value_type>::value,
                  "RWKeyType do not match.");

    // lock write set
    if (lockWriteSet(txn, syncMessages)) {
      abort(txn, syncMessages);
      return false;
    }

    // read epoch E
    txn.commitEpoch = epoch.load();

    // commit phase 2, read validation
    if (!validateReadSet(txn, syncMessages)) {
      abort(txn, syncMessages);
      return false;
    }

    // generate tid
    uint64_t commit_tid = generateTid(txn);

    // write
    write(txn, commit_tid, syncMessages);

    return true;
  }

private:
  template <class Transaction>
  bool lockWriteSet(Transaction &txn,
                    std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      // lock local records
      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        bool success;
        uint64_t latestTid = SiloHelper::lock(tid, success);

        if (!success) {
          txn.abort_lock = true;
          break;
        }

        writeKey.set_lock_bit();

        auto readKeyPtr = txn.get_read_key(key);
        // assume no blind write
        CHECK(readKeyPtr != nullptr);
        uint64_t tidOnRead = readKeyPtr->get_tid();
        if (latestTid != tidOnRead) {
          txn.abort_lock = true;
          break;
        }

        writeKey.set_tid(latestTid);
      } else {
        txn.pendingResponses++;
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        MessageFactoryType<TableType>::new_lock_message(
            *messages[coordinatorID], *table, writeKey.get_key(), i);
      }
    }

    txn.messageFlusher();
    while (txn.pendingResponses > 0) {
      txn.remoteRequestHandler();
    }

    return txn.abort_lock;
  }

  template <class Transaction>
  bool validateReadSet(Transaction &txn,
                       std::vector<std::unique_ptr<Message>> &messages) {

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

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      bool in_write_set = isKeyInWriteSet(readKey.get_key());
      if (in_write_set) {
        continue; // already validated in lock write set
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (partitioner.has_master_partition(partitionId)) {
        auto key = readKey.get_key();
        uint64_t tid = table->search_metadata(key).load();
        if (SiloHelper::removeLockBit(tid) != readKey.get_tid()) {
          txn.abort_read_validation = true;
          break;
        }
        if (SiloHelper::isLocked(tid)) { // must be locked by others
          txn.abort_read_validation = true;
          break;
        }
      } else {
        txn.pendingResponses++;
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        MessageFactoryType<TableType>::new_read_validation_message(
            *messages[coordinatorID], *table, readKey.get_key(), i,
            readKey.get_tid());
      }
    }

    txn.messageFlusher();
    while (txn.pendingResponses > 0) {
      txn.remoteRequestHandler();
    }

    return !txn.abort_read_validation;
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

  template <class Transaction>
  void write(Transaction &txn, uint64_t commit_tid,
             std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();

        std::atomic<uint64_t> &tid = table->search_metadata(key);
        table->update(key, value);
        SiloHelper::unlock(tid, commit_tid);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        MessageFactoryType<TableType>::new_write_message(
            *messages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value(), commit_tid);
      }
    }
    txn.messageFlusher();
  }

private:
  DatabaseType &db;
  std::atomic<uint64_t> &epoch;
  Partitioner &partitioner;
  uint64_t maxTID = 0;
};

} // namespace scar
