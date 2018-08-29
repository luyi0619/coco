//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include <atomic>
#include <vector>

#include "core/Worker.h"

#include <glog/logging.h>

namespace scar {
class IncomingDispatcher {

public:
  IncomingDispatcher(const std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     std::atomic<bool> &stopFlag)
      : sockets(sockets), workers(workers), stopFlag(stopFlag) {}

  void start() {

    while (!stopFlag.load()) {
    }

    LOG(INFO) << "IncomingDispatcher exits.";
  }

private:
  std::vector<Socket> sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  std::atomic<bool> &stopFlag;
};

class OutgoingDispatcher {
public:
  OutgoingDispatcher(const std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     std::atomic<bool> &stopFlag)
      : sockets(sockets), workers(workers), stopFlag(stopFlag) {}

  void start() {

    while (!stopFlag.load()) {
    }

    LOG(INFO) << "OutgoingDispatcher exits.";
  }

private:
  std::vector<Socket> sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  std::atomic<bool> &stopFlag;
};

} // namespace scar
