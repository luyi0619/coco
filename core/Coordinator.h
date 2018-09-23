//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/ControlMessage.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Worker.h"
#include "core/factory/WorkerFactory.h"
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <thread>
#include <vector>

namespace scar {

class Coordinator {
public:
  template <class Database, class Context>
  Coordinator(std::size_t id, const std::vector<std::string> &peers,
              Database &db, const Context &context)
      : id(id), coordinator_num(peers.size()), peers(peers) {
    workerStopFlag.store(false);
    ioStopFlag.store(false);
    LOG(INFO) << "Coordinator initializes " << context.worker_num
              << " workers.";
    workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);
  }

  ~Coordinator() = default;

  void start() {

    // start dispatcher threads
    iDispatcher = std::make_unique<IncomingDispatcher>(id, inSockets, workers,
                                                       in_queue, ioStopFlag);
    oDispatcher = std::make_unique<OutgoingDispatcher>(id, outSockets, workers,
                                                       out_queue, ioStopFlag);

    std::thread iDispatcherThread(&IncomingDispatcher::start,
                                  iDispatcher.get());
    std::thread oDispatcherThread(&OutgoingDispatcher::start,
                                  oDispatcher.get());

    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

    for (auto i = 0u; i < workers.size(); i++) {
      threads.emplace_back(&Worker::start, workers[i].get());
    }

    // run timeToRun seconds
    auto timeToRun = 30, warmup = 10, cooldown = 10;
    auto startTime = std::chrono::steady_clock::now();

    uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0,
             total_abort_read_validation = 0, total_network_size = 0;
    int count = 0;

    do {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0,
               n_abort_read_validation = 0, n_network_size = 0;

      for (auto i = 0u; i < workers.size(); i++) {

        n_commit += workers[i]->n_commit.load();
        workers[i]->n_commit.store(0);

        n_abort_no_retry += workers[i]->n_abort_no_retry.load();
        workers[i]->n_abort_no_retry.store(0);

        n_abort_lock += workers[i]->n_abort_lock.load();
        workers[i]->n_abort_lock.store(0);

        n_abort_read_validation += workers[i]->n_abort_read_validation.load();
        workers[i]->n_abort_read_validation.store(0);

        n_network_size += workers[i]->n_network_size.load();
        workers[i]->n_network_size.store(0);
      }

      LOG(INFO) << "commit: " << n_commit << " abort: "
                << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                << " (" << n_abort_no_retry << "/" << n_abort_lock << "/"
                << n_abort_read_validation
                << "), network size: " << n_network_size
                << " avg network size: " << 1.0 * n_network_size / n_commit;
      count++;
      if (count > warmup && count <= timeToRun - cooldown) {
        total_commit += n_commit;
        total_abort_no_retry += n_abort_no_retry;
        total_abort_lock += n_abort_lock;
        total_abort_read_validation += n_abort_read_validation;
        total_network_size += n_network_size;
      }

    } while (std::chrono::duration_cast<std::chrono::seconds>(
                 std::chrono::steady_clock::now() - startTime)
                 .count() < timeToRun);

    count = timeToRun - warmup - cooldown;

    LOG(INFO) << "average commit: " << 1.0 * total_commit / count << " abort: "
              << 1.0 *
                     (total_abort_no_retry + total_abort_lock +
                      total_abort_read_validation) /
                     count
              << " (" << 1.0 * total_abort_no_retry / count << "/"
              << 1.0 * total_abort_lock / count << "/"
              << 1.0 * total_abort_read_validation / count
              << "), network size: " << total_network_size
              << " avg network size: "
              << 1.0 * total_network_size / total_commit;

    workerStopFlag.store(true);

    for (auto i = 0u; i < threads.size(); i++) {
      workers[i]->onExit();
      threads[i].join();
    }

    // gather throughput
    double sum_commit = gather(1.0 * total_commit / count);
    if (id == 0) {
      LOG(INFO) << "total commit: " << sum_commit;
    }

    // make sure all messages are sent
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ioStopFlag.store(true);
    iDispatcherThread.join();
    oDispatcherThread.join();

    LOG(INFO) << "Coordinator exits.";
  }

  void connectToPeers() {

    inSockets = std::vector<Socket>(peers.size());
    outSockets = std::vector<Socket>(peers.size());

    // single node test mode
    if (peers.size() == 1) {
      return;
    }

    auto getAddressPort = [](const std::string &addressPort) {
      std::vector<std::string> result;
      boost::algorithm::split(result, addressPort, boost::is_any_of(":"));
      return result;
    };

    // start a listener thread

    std::thread listenerThread([id = this->id, peers = this->peers,
                                &inSockets = this->inSockets, getAddressPort] {
      auto n = peers.size();
      std::vector<std::string> addressPort = getAddressPort(peers[id]);

      Listener l(addressPort[0].c_str(), atoi(addressPort[1].c_str()), 100);
      LOG(INFO) << "Coordinator " << id << " listening on " << peers[id];

      for (std::size_t i = 0; i < n - 1; i++) {
        Socket socket = l.accept();
        std::size_t c_id;
        socket.read_number(c_id);
        inSockets[c_id] = std::move(socket);
      }

      LOG(INFO) << "Listener on coordinator " << id << " exits.";
    });

    // connect to peers
    auto n = peers.size();
    constexpr std::size_t retryLimit = 5;

    for (auto i = 0u; i < n; i++) {
      if (i == id)
        continue;

      std::vector<std::string> addressPort = getAddressPort(peers[i]);
      for (auto k = 0u; k < retryLimit; k++) {
        Socket socket;

        int ret = socket.connect(addressPort[0].c_str(),
                                 atoi(addressPort[1].c_str()));
        if (ret == -1) {
          socket.close();
          if (k == retryLimit - 1) {
            LOG(FATAL) << "failed to connect to peers, exiting ...";
            exit(1);
          }

          // listener on the other side has not been set up.
          LOG(INFO) << "Coordinator " << id << " failed to connect " << i << "("
                    << peers[i] << "), retry in 5 seconds.";
          std::this_thread::sleep_for(std::chrono::seconds(5));
          continue;
        }

        socket.disable_nagle_algorithm();
        LOG(INFO) << "Coordinator " << id << " connected to " << i;
        socket.write_number(id);
        outSockets[i] = std::move(socket);
        break;
      }
    }

    listenerThread.join();
    LOG(INFO) << "Coordinator " << id << " connected to all peers.";
  }

  double gather(double value) {

    auto init_message = [](Message *message, std::size_t coordinator_id,
                           std::size_t dest_node_id) {
      message->set_source_node_id(coordinator_id);
      message->set_dest_node_id(dest_node_id);
      message->set_worker_id(0);
    };

    double sum = value;

    if (id == 0) {
      for (std::size_t i = 0; i < coordinator_num - 1; i++) {

        in_queue.wait_till_non_empty();
        std::unique_ptr<Message> message(in_queue.front());
        bool ok = in_queue.pop();
        CHECK(ok);
        CHECK(message->get_message_count() == 1);

        MessagePiece messagePiece = *(message->begin());

        CHECK(messagePiece.get_message_type() ==
              static_cast<uint32_t>(ControlMessage::STATISTICS));
        CHECK(messagePiece.get_message_length() ==
              MessagePiece::get_header_size() + sizeof(double));
        Decoder dec(messagePiece.toStringPiece());
        double v;
        dec >> v;
        sum += v;
      }

    } else {
      auto message = std::make_unique<Message>();
      init_message(message.get(), id, 0);
      ControlMessageFactory::new_statistics_message(*message, value);
      out_queue.push(message.release());
    }
    return sum;
  }

private:
  std::size_t id, coordinator_num;
  std::vector<std::string> peers;
  std::vector<Socket> inSockets, outSockets;
  std::atomic<bool> workerStopFlag, ioStopFlag;
  std::vector<std::shared_ptr<Worker>> workers;
  std::unique_ptr<IncomingDispatcher> iDispatcher;
  std::unique_ptr<OutgoingDispatcher> oDispatcher;
  LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace scar
