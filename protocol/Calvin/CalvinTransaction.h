//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Defs.h"
#include "protocol/Calvin/CalvinPartitioner.h"
#include "protocol/Calvin/CalvinRWKey.h"
#include <chrono>
#include <glog/logging.h>

namespace scar {
class CalvinTransaction {

public:
  using MetaDataType = std::atomic<uint64_t>;

  CalvinTransaction(std::size_t coordinator_id, std::size_t worker_id,
                    std::size_t partition_id, Partitioner &partitioner)
      : coordinator_id(coordinator_id), worker_id(worker_id),
        partition_id(partition_id), startTime(std::chrono::steady_clock::now()),
        partitioner(partitioner) {
    reset();
  }

  virtual ~CalvinTransaction() = default;

  void reset() {
    pendingResponses.store(0);
    abort_lock = false;
    readSet.clear();
    writeSet.clear();
  }

  virtual TransactionResult execute() = 0;

  std::size_t add_to_read_set(const CalvinRWKey &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const CalvinRWKey &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::size_t coordinator_id, worker_id, partition_id;
  std::chrono::steady_clock::time_point startTime;
  std::atomic<int> pendingResponses; // could be negative

  bool abort_lock, abort_read_validation;

  // processed a request?
  std::function<std::size_t(void)> remote_request_handler;

  std::function<void()> message_flusher;

  Partitioner &partitioner;
  std::vector<CalvinRWKey> readSet, writeSet;
};
} // namespace scar