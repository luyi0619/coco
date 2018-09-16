//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/Socket.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Worker.h"
#include "core/factory/WorkerFactory.h"
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <thread>
#include <vector>

namespace scar {

class ICoordinator {
public:
  ~ICoordinator() = default;

  virtual void start() = 0;

  virtual void connectToPeers() = 0;
};

template <class Workload> class Coordinator : public ICoordinator {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  Coordinator(std::size_t id, const std::vector<std::string> &peers,
              DatabaseType &db, ContextType &context)
      : id(id), peers(peers), db(db), context(context) {
    workerStopFlag.store(false);
    ioStopFlag.store(false);
  }

  ~Coordinator() = default;

  void start() override {

    LOG(INFO) << "Coordinator initializes " << context.worker_num
              << " workers.";

    workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);

    // start dispatcher threads
    iDispatcher = std::make_unique<IncomingDispatcher>(id, inSockets, workers,
                                                       ioStopFlag);
    oDispatcher = std::make_unique<OutgoingDispatcher>(id, outSockets, workers,
                                                       ioStopFlag);

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
    auto timeToRun = 500000, warmup = 10, cooldown = 10;
    auto startTime = std::chrono::steady_clock::now();

    uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0,
             total_abort_read_validation = 0;
    int count = 0;

    do {
      std::this_thread::sleep_for(std::chrono::seconds(1));

      uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0,
               n_abort_read_validation = 0;

      for (auto i = 0u; i < workers.size(); i++) {

        n_commit += workers[i]->n_commit.load();
        workers[i]->n_commit.store(0);

        n_abort_no_retry += workers[i]->n_abort_no_retry.load();
        workers[i]->n_abort_no_retry.store(0);

        n_abort_lock += workers[i]->n_abort_lock.load();
        workers[i]->n_abort_lock.store(0);

        n_abort_read_validation += workers[i]->n_abort_read_validation.load();
        workers[i]->n_abort_read_validation.store(0);
      }

      LOG(INFO) << "commit: " << n_commit << " abort: "
                << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                << " (" << n_abort_no_retry << "/" << n_abort_lock << "/"
                << n_abort_read_validation << ")";
      count++;
      if (count > warmup && count <= timeToRun - cooldown) {
        total_commit += n_commit;
        total_abort_no_retry += n_abort_no_retry;
        total_abort_lock += n_abort_lock;
        total_abort_read_validation += n_abort_read_validation;
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
              << 1.0 * total_abort_read_validation / count << ")";

    workerStopFlag.store(true);

    for (auto i = 0u; i < threads.size(); i++) {
      workers[i]->onExit();
      threads[i].join();
    }

    ioStopFlag.store(true);
    iDispatcherThread.join();
    oDispatcherThread.join();

    LOG(INFO) << "Coordinator exits.";
  }

  void connectToPeers() override {

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

private:
  std::size_t id;
  std::vector<std::string> peers;
  std::vector<Socket> inSockets, outSockets;
  std::atomic<bool> workerStopFlag, ioStopFlag;
  DatabaseType &db;
  ContextType &context;
  std::vector<std::shared_ptr<Worker>> workers;
  std::unique_ptr<IncomingDispatcher> iDispatcher;
  std::unique_ptr<OutgoingDispatcher> oDispatcher;
};
} // namespace scar
