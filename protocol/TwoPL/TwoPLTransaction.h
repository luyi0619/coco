//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "common/Message.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace scar {
template <class Database> class TwoPLTransaction {

public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;
  using TableType = ITable<MetaDataType>;

  TwoPLTransaction(std::size_t coordinator_id, std::size_t worker_id,
                   std::size_t partition_id, DatabaseType &db,
                   const ContextType &context, RandomType &random,
                   Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id),
        partition_id(partition_id), startTime(std::chrono::steady_clock::now()),
        db(db), context(context), random(random), partitioner(partitioner) {
    reset();
  }

  virtual ~TwoPLTransaction() = default;

  void reset() {
    pendingResponses = 0;
    abort_lock = false;
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(partition_id)) {
      pendingResponses++;
    }

    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {

    if (!partitioner.has_master_partition(partition_id)) {
      pendingResponses++;
    }

    TwoPLRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_write_request_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {

    TwoPLRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  void process_read_request() {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (readSet[i].get_read_request_lock_bit() &&
          readSet[i].get_write_request_lock_bit()) {
        break;
      }

      const TwoPLRWKey &readKey = readSet[i];
      auto tid = lock_request_handler(readKey.get_table_id(),
                                      readKey.get_partition_id(), i,
                                      readKey.get_key(), readKey.get_value());
      readSet[i].set_tid(tid);
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
      }
    }
  }

  SiloRWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const SiloRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const SiloRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, worker_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;

  bool abort_lock;

  // table id, partition id, key, value
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *,
                         void *)>
      lock_request_handler;
  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Partitioner &partitioner;
  std::vector<TwoPLRWkey> readSet, writeSet;
};
} // namespace scar