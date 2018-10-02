//
// Created by Yi Lu on 9/19/18.
//

#pragma once

#include "common/Message.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Scar/ScarRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace scar {

class ScarTransaction {
public:
  using MetaDataType = std::atomic<uint64_t>;

  ScarTransaction(std::size_t coordinator_id, std::size_t partition_id,
                  Partitioner &partitioner)
      : coordinator_id(coordinator_id), partition_id(partition_id),
        startTime(std::chrono::steady_clock::now()), partitioner(partitioner) {
    reset();
  }

  virtual ~ScarTransaction() = default;

  void reset() {
    pendingResponses = 0;
    network_size = 0;
    abort_lock = false;
    abort_read_validation = false;
    si_in_serializable = false;
    local_validated = false;
    operation.clear();
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute() = 0;

  template <class KeyType, class ValueType>
  void search_local_index(std::size_t table_id, std::size_t partition_id,
                          const KeyType &key, ValueType &value) {
    ScarRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_local_index_read_bit();
    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_read(std::size_t table_id, std::size_t partition_id,
                       const KeyType &key, ValueType &value) {

    ScarRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void search_for_update(std::size_t table_id, std::size_t partition_id,
                         const KeyType &key, ValueType &value) {

    ScarRWKey readKey;

    readKey.set_table_id(table_id);
    readKey.set_partition_id(partition_id);

    readKey.set_key(&key);
    readKey.set_value(&value);

    readKey.set_read_request_bit();

    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    ScarRWKey writeKey;

    writeKey.set_table_id(table_id);
    writeKey.set_partition_id(partition_id);

    writeKey.set_key(&key);
    // the object pointed by value will not be updated
    writeKey.set_value(const_cast<ValueType *>(&value));

    add_to_write_set(writeKey);
  }

  bool process_requests() {

    // cannot use unsigned type in reverse iteration
    for (int i = int(readSet.size()) - 1; i >= 0; i--) {
      // early return
      if (!readSet[i].get_read_request_bit()) {
        break;
      }

      const ScarRWKey &readKey = readSet[i];
      auto tid =
          readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(),
                             i, readKey.get_key(), readKey.get_value(),
                             readKey.get_local_index_read_bit());
      readSet[i].clear_read_request_bit();
      readSet[i].set_tid(tid);
    }

    if (pendingResponses > 0) {
      message_flusher();
      while (pendingResponses > 0) {
        remote_request_handler();
      }
    }
    return false;
  }

  ScarRWKey *get_read_key(const void *key) {

    for (auto i = 0u; i < readSet.size(); i++) {
      if (readSet[i].get_key() == key) {
        return &readSet[i];
      }
    }

    return nullptr;
  }

  std::size_t add_to_read_set(const ScarRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const ScarRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::size_t pendingResponses;
  std::size_t network_size;
  uint64_t commit_rts, commit_wts;
  bool abort_lock, abort_read_validation, local_validated, si_in_serializable;

  // table id, partition id, key, value, local index read?
  std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *,
                         void *, bool)>
      readRequestHandler;
  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  Operation operation;
  std::vector<ScarRWKey> readSet, writeSet;
};

} // namespace scar
