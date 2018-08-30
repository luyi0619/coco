//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/Socket.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Worker.h"
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <thread>
#include <vector>

namespace scar {

template <class Workload> class Coordinator {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using ProtocolType = typename WorkloadType::ProtocolType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  Coordinator(std::size_t id, const std::vector<std::string> &peers,
              DatabaseType &db, ContextType &context)
      : id(id), peers(peers),
        db(db), context(context) {
    epoch.store(0);
    workerStopFlag.store(false);
    epochStopFlag.store(false);
    ioStopFlag.store(false);
  }

  void start() {

    LOG(INFO) << "Coordinator initializes " << context.workerNum << " workers.";

    for (auto i = 0u; i < context.workerNum; i++) {
      workers.push_back(std::make_shared<Executor<WorkloadType>>(
          i, db, context, epoch, workerStopFlag));
    }

    std::thread epochThread(&Coordinator::advanceEpoch, this);

    // start dispatcher threads
    iDispatcher = std::make_unique<IncomingDispatcher>(id, std::move(inSockets), workers,
                                                       ioStopFlag);
    oDispatcher = std::make_unique<OutgoingDispatcher>(id, std::move(outSockets), workers,
                                                       ioStopFlag);

    std::thread iDispatcherThread(&IncomingDispatcher::start,
                                  iDispatcher.get());
    std::thread oDispatcherThread(&OutgoingDispatcher::start,
                                  oDispatcher.get());

    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << context.workerNum
              << " workers.";

    for (auto i = 0u; i < context.workerNum; i++) {
      threads.emplace_back(&Worker::start, workers[i].get());
    }

    // run timeToRun milliseconds
    auto timeToRun = 1000;
    LOG(INFO) << "Coordinator starts to sleep " << timeToRun
              << " milliseconds.";
    std::this_thread::sleep_for(std::chrono::milliseconds(timeToRun));
    workerStopFlag.store(true);
    LOG(INFO) << "Coordinator awakes.";

    uint64_t totalTransaction = 0;

    for (auto i = 0u; i < context.workerNum; i++) {
      threads[i].join();
      workers[i]->onExit();
      totalTransaction += workers[i]->transactionId + 1;
    }

    epochStopFlag.store(true);
    epochThread.join();

    ioStopFlag.store(true);
    iDispatcherThread.join();
    oDispatcherThread.join();

    LOG(INFO) << "Coordinator executed " << totalTransaction
              << " transactions in " << timeToRun << " milliseconds.";

    LOG(INFO) << "Coordinator exits.";
  }

  void connectToPeers() {

    if (peers.size() <= 1)
      return;

    inSockets = std::vector<Socket>(peers.size());
    outSockets = std::vector<Socket>(peers.size());

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
      LOG(INFO) << "Coordinator " << id << " "
                << " listening on " << peers[id];

      for (std::size_t i = 0; i < n - 1; i++) {
        Socket socket = l.accept();
        int c_id;
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

        int ret =
            socket.connect(addressPort[0].c_str(), atoi(addressPort[1].c_str()));
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

private:
  void advanceEpoch() {

    LOG(INFO) << "Coordinator epoch thread starts.";

    auto sleepTime = std::chrono::milliseconds(40);

    // epoch thread only exits when worker threads have exited, making epoch is
    // larger than the epoch workers read

    while (!epochStopFlag.load()) {
      std::this_thread::sleep_for(sleepTime);
      epoch.fetch_add(1);
    }

    LOG(INFO) << "Coordinator epoch thread exits, last epoch = " << epoch.load()
              << ".";
  }

private:
  std::size_t id;
  std::vector<std::string> peers;
  std::vector<Socket> inSockets, outSockets;
  std::atomic<uint64_t> epoch;
  std::atomic<bool> workerStopFlag, epochStopFlag, ioStopFlag;
  DatabaseType &db;
  ContextType &context;
  std::vector<std::shared_ptr<Worker>> workers;
  std::unique_ptr<IncomingDispatcher> iDispatcher;
  std::unique_ptr<OutgoingDispatcher> oDispatcher;
};
} // namespace scar
