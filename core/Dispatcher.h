//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/Worker.h"
#include <atomic>
#include <glog/logging.h>
#include <vector>

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

    auto numCoordinators = sockets.size();
    auto numWorkers = workers.size();
    LOG(INFO) << "Outgoing Dispatcher started, numCoordinators = "
              << numCoordinators << " numWorkers = " << numWorkers;

    while (!stopFlag.load()) {

      for (auto i = 0u; i < numWorkers; i++) {
        dispatchMessage(workers[i]);
      }
    }

    LOG(INFO) << "Outgoing Dispatcher exits.";
  }

  void dispatchMessage(const std::shared_ptr<Worker> &worker) {

    LockfreeQueue<Message *> &queue = worker->outQueue;
    if (queue.empty())
      return;

    // wrap the message with a unique pointer.
    std::unique_ptr<Message> message(queue.front());
    bool ok = queue.pop();
    CHECK(ok);

    // send the message
    auto dest_node_id = message->get_dest_node_id();
    CHECK(dest_node_id >= 0 && dest_node_id < sockets.size() &&
          dest_node_id != id);
    CHECK(message->get_message_length() == message->data.length());
    sockets[dest_node_id].write_n_bytes(message->get_raw_ptr(),
                                        message->get_message_length());
  }

private:
  std::size_t id;
  std::vector<Socket> sockets;
  std::vector<std::shared_ptr<Worker>> workers;
  std::atomic<bool> &stopFlag;
};

} // namespace scar
