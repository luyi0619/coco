//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/Message.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace scar {

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

template <class RWKey, class Database> class Transaction {
public:
  using RWKeyType = RWKey;
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;
  using TableType = ITable<MetaDataType>;

  Transaction(std::size_t coordinator_id, std::size_t worker_id,
              DatabaseType &db, ContextType &context, RandomType &random,
              Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id),
        startTime(std::chrono::steady_clock::now()), commitEpoch(0),
        pendingResponses(0), abort_lock(false), abort_read_validation(false),
        db(db), context(context), random(random), partitioner(partitioner) {}

  virtual ~Transaction() = default;

  virtual TransactionResult execute() = 0;

  template <class KeyType, class ValueType>
  void search(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(partition_id)) {
      pendingResponses++;
    }

    RWKeyType readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    RWKeyType writeKey;
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

      const RWKeyType &readKey = readSet[i];
      auto tid =
          readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value());
      readSet[i].set_read_request_bit();
      readSet[i].set_tid(tid);
    }

    messageFlusher();

    while (pendingResponses > 0) {
      remoteRequestHandler();
    }
  }

  RWKeyType *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const RWKeyType &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const RWKeyType &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, worker_id;
  std::chrono::steady_clock::time_point startTime;
  uint64_t commitEpoch;
  std::size_t pendingResponses;

  bool abort_lock, abort_read_validation;

  // table id, partition id, key, value
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *,
                         void *)>
      readRequestHandler;
  // processed a request?
  std::function<std::size_t(void)> remoteRequestHandler;

  std::function<void()> messageFlusher;

  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  Partitioner &partitioner;
  std::vector<RWKeyType> readSet, writeSet;
};

} // namespace scar
