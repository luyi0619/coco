//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinPartitioner.h"
#include "protocol/Calvin/CalvinRWKey.h"
#include <chrono>
#include <glog/logging.h>

namespace scar {
class CalvinTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;
  using TableType = ITable<MetaDataType>;

  CalvinTransaction(std::size_t coordinator_id, std::size_t partition_id,
                    Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~CalvinTransaction() = default;

  void reset() {
    pendingResponses = 0;
    execution_phase = false;
    active_coordinators.clear();
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {

    if (execution_phase) {
      return;
    }

    CalvinRWKey readKey;

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

    if (execution_phase) {
      return;
    }

    CalvinRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {
    if (execution_phase) {
      return;
    }

    CalvinRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_write_lock_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {

    if (!execution_phase) {
      return;
    }

    CalvinRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const CalvinRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const CalvinRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

  void set_id(std::size_t id) { this->id = id; }

  void setup_process_requests_in_prepare_phase() {
    process_requests = [this]() {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (readSet[i].get_prepare_processed_bit()) {
          break;
        }

        if (readSet[i].get_local_index_read_bit()) {
          // this is a local index read
          auto &readKey = readSet[i];
          local_index_read_handler(readKey.get_table_id(),
                                   readKey.get_partition_id(),
                                   readKey.get_key(), readKey.get_value());
        }

        readSet[i].set_prepare_processed_bit();
      }
      return true;
    };
  }

  void setup_process_requests_in_execution_phase() {
    process_requests = [this]() {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (readSet[i].get_execution_processed_bit()) {
          break;
        }

        if (!readSet[i].get_local_index_read_bit()) {
          // this is a local index read
          auto &readKey = readSet[i];
          read_handler(readKey.get_table_id(), readKey.get_partition_id(), id,
                       i, readKey.get_key(), readKey.get_value());
        }

        readSet[i].set_execution_processed_bit();
      }

      if (pendingResponses != 0) {
        message_flusher();
        while (pendingResponses != 0) {
          remote_request_handler();
        }
      }
      return false;
    };
  }

public:
  std::size_t coordinator_id, partition_id, id;
  std::chrono::steady_clock::time_point startTime;
  int32_t pendingResponses; // could be negative

  bool abort_lock, abort_read_validation;
  bool execution_phase;

  std::function<bool(void)> process_requests;

  // table id, partition id, key, value
  std::function<void(std::size_t, std::size_t, const void *, void *)>
      local_index_read_handler;

  // table id, partition id, id, key_offset, key, value
  std::function<void(std::size_t, std::size_t, std::size_t, uint32_t,
                     const void *, void *)>
      read_handler;

  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  std::vector<std::size_t> active_coordinators;
  std::vector<CalvinRWKey> readSet, writeSet;
};
} // namespace scar