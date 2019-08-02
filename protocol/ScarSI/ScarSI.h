//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Scar/ScarHelper.h"
#include "protocol/Scar/ScarTransaction.h"
#include "protocol/ScarGC/ScarGCMessage.h"
#include <glog/logging.h>

namespace scar {

template <class Database> class ScarSI {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = ScarGCMessage;
  using TransactionType = ScarTransaction;

  using MessageFactoryType = ScarGCMessageFactory;
  using MessageHandlerType = ScarGCMessageHandler;

  ScarSI(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  uint64_t search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {

    ITable *table = db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    return ScarHelper::read(row, value, value_bytes);
  }

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &syncMessages,
             std::vector<std::unique_ptr<Message>> &asyncMessage) {

    auto &writeSet = txn.writeSet;

    // unlock locked records

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      // only unlock locked records
      if (!writeKey.get_write_lock_bit())
        continue;
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        ScarHelper::unlock(tid);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_abort_message(
            *syncMessages[coordinatorID], *table, writeKey.get_key());
      }
    }

    replicate_read_set(txn, asyncMessage, false);

    if (context.rts_sync) {
      replicate_rts(txn, asyncMessage);
    }

    sync_messages(txn, false);
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &syncMessages,
              std::vector<std::unique_ptr<Message>> &asyncMessage) {

    compute_commit_rts(txn);

    // lock write set
    if (validate_read_set_and_lock_write_set(txn, syncMessages)) {
      abort(txn, syncMessages, asyncMessage);
      return false;
    }

    compute_commit_wts(txn);

    if (txn.commit_rts == txn.commit_wts) {
      txn.si_in_serializable = true;
    }

    // write and replicate
    write_and_replicate(txn, syncMessages, asyncMessage);
    return true;
  }

private:
  bool validate_read_set_and_lock_write_set(
      TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    bool use_local_validation = context.local_validation;
    uint64_t commit_ts = txn.commit_rts;

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

      if (readKey.get_local_index_read_bit()) {
        continue; // read only index does not need to validate
      }

      bool in_write_set = isKeyInWriteSet(readKey.get_key());
      if (in_write_set) {
        continue; // already validated in lock write set
      }

      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);
      auto key = readKey.get_key();
      uint64_t tid = readKey.get_tid();

      if (partitioner.has_master_partition(partitionId)) {

        std::atomic<uint64_t> &latest_tid = table->search_metadata(key);
        uint64_t written_ts = tid;
        DCHECK(ScarHelper::is_locked(written_ts) == false);
        if (ScarHelper::validate_read_key(latest_tid, tid, commit_ts,
                                          written_ts)) {
          readKey.set_read_validation_success_bit();
          if (ScarHelper::get_wts(written_ts) != ScarHelper::get_wts(tid)) {
            DCHECK(ScarHelper::get_wts(written_ts) > ScarHelper::get_wts(tid));
            readKey.set_wts_change_in_read_validation_bit();
            readKey.set_tid(written_ts);
          }
        } else {
          txn.abort_read_validation = true;
          break;
        }
      } else {

        if (!use_local_validation || ScarHelper::get_rts(tid) < commit_ts) {
          txn.pendingResponses++;
          auto coordinatorID = partitioner.master_coordinator(partitionId);
          txn.network_size += MessageFactoryType::new_read_validation_message(
              *messages[coordinatorID], *table, key, i, tid, commit_ts);
        }
      }
    }

    if (txn.pendingResponses == 0) {
      txn.local_validated = true;
    }

    if (context.parallel_locking_and_validation) {
      sync_messages(txn);
    }

    // lock records in any order. there might be dead lock.

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
        uint64_t latestTid = ScarHelper::lock(tid, success);

        if (!success) {
          txn.abort_lock = true;
          break;
        }

        writeKey.set_write_lock_bit();
        writeKey.set_tid(latestTid);

        auto readKeyPtr = txn.get_read_key(key);
        // assume no blind write
        DCHECK(readKeyPtr != nullptr);
        uint64_t tidOnRead = readKeyPtr->get_tid();
        if (ScarHelper::get_wts(latestTid) != ScarHelper::get_wts(tidOnRead)) {
          txn.abort_lock = true;
          break;
        }

      } else {
        txn.pendingResponses++;
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_lock_message(
            *messages[coordinatorID], *table, writeKey.get_key(), i);
      }
    }

    sync_messages(txn);

    return txn.abort_lock || txn.abort_read_validation;
  }

  void compute_commit_rts(TransactionType &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    uint64_t ts = 0;

    for (auto i = 0u; i < readSet.size(); i++) {
      ts = std::max(ts, ScarHelper::get_wts(readSet[i].get_tid()));
    }

    txn.commit_rts = ts;
  }

  void compute_commit_wts(TransactionType &txn) {

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    uint64_t ts = txn.commit_rts;

    for (auto i = 0u; i < writeSet.size(); i++) {
      ts = std::max(ts, ScarHelper::get_rts(writeSet[i].get_tid()) + 1);
    }

    txn.commit_wts = ts;
  }

  void
  write_and_replicate(TransactionType &txn,
                      std::vector<std::unique_ptr<Message>> &syncMessages,
                      std::vector<std::unique_ptr<Message>> &asyncMessage) {

    // no operation replication in Scar

    auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;

    uint64_t commit_wts = txn.commit_wts;

    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      // write
      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        table->update(key, value);
        ScarHelper::unlock(tid, commit_wts);
      } else {
        txn.pendingResponses++;
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_write_message(
            *syncMessages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value(), commit_wts);
      }

      // value replicate

      replicate_record(txn, asyncMessage, tableId, partitionId,
                       writeKey.get_key(), writeKey.get_value(), commit_wts);
    }

    replicate_read_set(txn, asyncMessage, true);

    if (context.rts_sync) {
      replicate_rts(txn, asyncMessage);
    }

    sync_messages(txn, false);
  }

  void replicate_read_set(TransactionType &txn,
                          std::vector<std::unique_ptr<Message>> &messages,
                          bool skip_write_set) {

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
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!readKey.get_wts_change_in_read_validation_bit()) {
        continue;
      }

      if (skip_write_set && isKeyInWriteSet(readKey.get_key())) {
        continue;
      }

      // value replicate
      replicate_record(txn, messages, tableId, partitionId, readKey.get_key(),
                       readKey.get_value(), readKey.get_tid());
    }
  }

  void replicate_rts(TransactionType &txn,
                     std::vector<std::unique_ptr<Message>> &messages) {

    auto &readSet = txn.readSet;
    uint64_t extend_rts = txn.commit_rts;

    for (auto i = 0u; i < readSet.size(); i++) {
      auto &readKey = readSet[i];
      auto tableId = readKey.get_table_id();
      auto partitionId = readKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      if (!readKey.get_read_validation_success_bit()) {
        continue;
      }

      if (extend_rts <= ScarHelper::get_rts(readKey.get_tid())) {
        continue;
      }

      for (auto k = 0u; k < partitioner.total_coordinators(); k++) {

        // k does not have this partition
        if (!partitioner.is_partition_replicated_on(partitionId, k)) {
          continue;
        }

        // already write
        if (k == partitioner.master_coordinator(partitionId)) {
          continue;
        }

        uint64_t compact_ts =
            ScarHelper::set_rts(readKey.get_tid(), extend_rts);
        if (ScarHelper::get_wts(compact_ts) !=
            ScarHelper::get_wts(readKey.get_tid())) {
          continue; // if delta is too larger, continue
        }

        CHECK(ScarHelper::get_wts(compact_ts) ==
              ScarHelper::get_wts(readKey.get_tid()));
        CHECK(ScarHelper::get_rts(compact_ts) == extend_rts);

        // local extend
        if (k == txn.coordinator_id) {
          std::atomic<uint64_t> &tid =
              table->search_metadata(readKey.get_key());
          uint64_t last_tid = ScarHelper::lock(tid);
          if (ScarHelper::get_wts(compact_ts) ==
                  ScarHelper::get_wts(last_tid) &&
              ScarHelper::get_rts(compact_ts) > ScarHelper::get_rts(last_tid)) {
            ScarHelper::unlock(tid, compact_ts);
          } else {
            ScarHelper::unlock(tid);
          }
        } else {
          auto coordinatorID = k;
          txn.network_size += MessageFactoryType::new_rts_replication_message(
              *messages[coordinatorID], *table, readKey.get_key(), compact_ts);
        }
      }
    }
  }

  void replicate_record(TransactionType &txn,
                        std::vector<std::unique_ptr<Message>> &messages,
                        std::size_t table_id, std::size_t partition_id,
                        const void *key, void *value, uint64_t commit_wts) {

    std::size_t replicate_count = 0;

    auto table = db.find_table(table_id, partition_id);
    for (auto k = 0u; k < partitioner.total_coordinators(); k++) {

      // k does not have this partition
      if (!partitioner.is_partition_replicated_on(partition_id, k)) {
        continue;
      }

      // already write
      if (k == partitioner.master_coordinator(partition_id)) {
        continue;
      }

      replicate_count++;

      // local replicate
      if (k == txn.coordinator_id) {
        std::atomic<uint64_t> &tid = table->search_metadata(key);
        uint64_t last_tid = ScarHelper::lock(tid);
        DCHECK(ScarHelper::get_wts(last_tid) < commit_wts);
        table->update(key, value);
        ScarHelper::unlock(tid, commit_wts);
      } else {
        auto coordinatorID = k;
        txn.pendingResponses++;
        txn.network_size += MessageFactoryType::new_replication_message(
            *messages[coordinatorID], *table, key, value, commit_wts);
      }
    }

    DCHECK(replicate_count == partitioner.replica_num() - 1);
  }

  void sync_messages(TransactionType &txn, bool wait_response = true) {
    txn.message_flusher();
    if (wait_response) {
      while (txn.pendingResponses > 0) {
        txn.remote_request_handler();
      }
    }
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
};

} // namespace scar
