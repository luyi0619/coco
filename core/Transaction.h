//
// Created by Yi Lu on 7/22/18.
//

#pragma once

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

  Transaction(DatabaseType &db, ContextType &context, RandomType &random)
      : startTime(std::chrono::steady_clock::now()), commitEpoch(0),
        pendingRequests(0), db(db), context(context), random(random) {}

  virtual ~Transaction() = default;

  virtual TransactionResult execute() = 0;

  template <class KeyType, class ValueType>
  void search(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, ValueType &value) {

    pendingRequests ++;

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

      // process local read;
      const RWKeyType &readKey = readSet[i];
      readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                         readKey.get_key(), readKey.get_value());
      readSet[i].set_read_request_bit();

      pendingRequests--;
    }

    CHECK(pendingRequests == 0);

    while (pendingRequests > 0) {
      if (remoteRequestHandler()) {
        pendingRequests--;
      }
    }
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
  std::chrono::steady_clock::time_point startTime;
  uint64_t commitEpoch;
  std::size_t pendingRequests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)>
      readRequestHandler;
  // processed a request?
  std::function<bool(void)> remoteRequestHandler;

  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  std::vector<RWKeyType> readSet, writeSet;
};

} // namespace scar
