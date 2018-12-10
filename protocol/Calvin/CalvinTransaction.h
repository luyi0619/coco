//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

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
    abort_lock = false;
    abort_no_retry = false;
    abort_read_validation = false;
    local_validated = false;
    si_in_serializable = false;
    local_read.store(0);
    remote_read.store(0);
    execution_phase = false;
    network_size = 0;
    active_coordinators.clear();
    operation.clear();
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
    // process the reads in read-only index
    // for general reads, increment the local_read and remote_read counter.
    // the function may be called multiple times, the keys are processed in
    // reverse order.
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
        } else {

          if (partitioner.has_master_partition(readSet[i].get_partition_id())) {
            local_read.fetch_add(1);
          } else {
            remote_read.fetch_add(1);
          }
        }

        readSet[i].set_prepare_processed_bit();
      }
      return true;
    };
  }

  void
  setup_process_requests_in_execution_phase(std::size_t lock_manager_id,
                                            std::size_t n_lock_manager,
                                            std::size_t replica_group_size) {
    // only read the keys with locks from the lock_manager_id
    process_requests = [this, lock_manager_id, n_lock_manager,
                        replica_group_size]() {
      // cannot use unsigned type in reverse iteration
      for (int i = int(readSet.size()) - 1; i >= 0; i--) {
        // early return
        if (readSet[i].get_execution_processed_bit()) {
          break;
        }

        if (readSet[i].get_local_index_read_bit()) {
          continue;
        }

        if (CalvinHelper::partition_id_to_lock_manager_id(
                readSet[i].get_partition_id(), n_lock_manager,
                replica_group_size) != lock_manager_id) {
          continue;
        }

        auto &readKey = readSet[i];
        read_handler(readKey.get_table_id(), readKey.get_partition_id(), id, i,
                     readKey.get_key(), readKey.get_value());

        readSet[i].set_execution_processed_bit();
      }

      message_flusher();

      if (active_coordinators[coordinator_id]) {

        // spin on local_read
        while (local_read.load() > 0) {
          std::this_thread::yield();
        }

        // spin on remote read

        while (remote_read.load() > 0) {
          remote_request_handler();
        }
        return false;
      } else {
        // abort if not active
        return true;
      }
    };
  }

public:
  std::size_t coordinator_id, partition_id, id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t network_size;
  std::atomic<int32_t> local_read, remote_read;

  bool abort_lock, abort_no_retry, abort_read_validation, local_validated,
      si_in_serializable;
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
  std::vector<bool> active_coordinators;
  Operation operation; // never used
  std::vector<CalvinRWKey> readSet, writeSet;
};
} // namespace scar