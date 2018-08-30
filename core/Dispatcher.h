//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include <atomic>
#include <vector>

#include "common/Message.h"
#include "core/Worker.h"

#include <glog/logging.h>

namespace scar {
class IncomingDispatcher {

public:
  IncomingDispatcher(std::size_t id, const std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     std::atomic<bool> &stopFlag)
      : id(id), sockets(sockets), workers(workers), stopFlag(stopFlag) {}

  void start() {
    auto numCoordinators = sockets.size();
    auto numWorkers = workers.size();
    LOG(INFO) << "Incoming Dispatcher started, numCoordinators = "
              << numCoordinators << " numWorkers = " << numWorkers;

    while (!stopFlag.load()) {

      for (auto i = 0u; i < numCoordinators; i++) {
        if (i == id) {
          continue;
        }

        auto message = fetchMessage(sockets[i]);

        if (message == nullptr) {
          continue;
        }
      }
    }

    LOG(INFO) << "Incoming Dispatcher exits.";
  }

  std::unique_ptr<Message> fetchMessage(Socket &socket) { return nullptr; }

private:
  std::size_t id;
  std::vector<Socket> sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  std::atomic<bool> &stopFlag;
};

class OutgoingDispatcher {
public:
  OutgoingDispatcher(std::size_t id, const std::vector<Socket> &sockets,
                     const std::vector<std::shared_ptr<Worker>> &workers,
                     std::atomic<bool> &stopFlag)
      : id(id), sockets(sockets), workers(workers), stopFlag(stopFlag) {}

  void start() {

    while (!stopFlag.load()) {
    }

    LOG(INFO) << "Outgoing Dispatcher exits.";
  }

private:
  std::size_t id;
  std::vector<Socket> sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  std::atomic<bool> &stopFlag;
};

} // namespace scar
