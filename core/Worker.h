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
  Worker(std::size_t id) : id(id) { transactionId.store(0); }

  virtual void start() = 0;

  virtual void onExit() {}

public:
  std::size_t id;
  LockfreeQueue<Message *> inQueue, outQueue;
  std::atomic<uint64_t> transactionId;
};

} // namespace scar
