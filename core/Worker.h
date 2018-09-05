//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "core/Transaction.h"
#include <atomic>
#include <glog/logging.h>
#include <queue>

namespace scar {

class Worker {
public:
  Worker(std::size_t coordinator_id, std::size_t id)
      : coordinator_id(coordinator_id), id(id) {
    n_commit.store(0);
    n_abort.store(0);
  }

  virtual void start() = 0;

  virtual void onExit() {}

public:
  std::size_t coordinator_id;
  std::size_t id;
  LockfreeQueue<Message *> inQueue, outQueue;
  std::atomic<uint64_t> n_commit, n_abort;
};

} // namespace scar
