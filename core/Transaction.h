//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/Message.h"
#include "core/Partitioner.h"
#include "core/RWKey.h"
#include "core/Table.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace scar {

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

template <class Database> class Transaction {
public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;
  using TableType = ITable<MetaDataType>;

  Transaction(std::size_t coordinator_id, std::size_t worker_id,
              std::size_t partition_id, DatabaseType &db,
              const ContextType &context, RandomType &random,
              Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id),
        partition_id(partition_id), startTime(std::chrono::steady_clock::now()),
        db(db), context(context), random(random), partitioner(partitioner) {
    reset();
  }

  virtual ~Transaction() = default;

  void reset() {
    pendingResponses = 0;
    abort_lock = false;
    abort_read_validation = false;
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute() = 0;

  template <class KeyType, class ValueType>
  void search(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, ValueType &value,
              bool local_index_read = false) {

    if (!local_index_read && !partitioner.has_master_partition(partition_id)) {
      pendingResponses++;
    }

    RWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    if (local_index_read) {
      readKey.set_local_index_read_bit();
    }

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    RWKey writeKey;
    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    TableType *table = db.find_table(table_id, partition_id);
    MetaDataType &metaData = table->search_metadata(&key);
    writeKey.set_sort_key(&metaData);

    add_to_write_set(writeKey);
  }

  void process_read_request() {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (readSet[i].get_read_request_bit()) {
        break;
      }

      const RWKey &readKey = readSet[i];
      auto tid =
          readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value(),
                             readKey.get_local_index_read_bit());
      readSet[i].set_read_request_bit();
      readSet[i].set_tid(tid);
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
      }
    }
  }

  RWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const RWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const RWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, worker_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;

  bool abort_lock, abort_read_validation;

  // table id, partition id, key, value
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *,
                         void *, bool)>
      readRequestHandler;
  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Partitioner &partitioner;
  std::vector<RWKey> readSet, writeSet;
};

} // namespace scar
