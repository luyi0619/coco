//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "core/Worker.h"
#include <glog/logging.h>
#include <vector>

namespace scar {

template <class Workload> class Coordinator {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using ProtocolType = typename WorkloadType::ProtocolType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  Coordinator(std::size_t id, DatabaseType &db, ContextType &context)
      : id(id), db(db), context(context) {
    epoch.store(0);
    stopFlag.store(false);
  }

  void start() {

    LOG(INFO) << "Coordinator initializes " << context.workerNum << " workers.";

    for (auto i = 0u; i < context.workerNum; i++) {
      workers.push_back(std::make_unique<Worker<WorkloadType>>(
          i, db, context, epoch, stopFlag));
    }

    std::thread epochThread(&Coordinator::advanceEpoch, this);

    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << context.workerNum
              << " workers.";

    for (auto i = 0u; i < context.workerNum; i++) {
      threads.emplace_back(&Worker<WorkloadType>::start, workers[i].get());
    }

    // run timeToRun milliseconds
    auto timeToRun = 100;
    LOG(INFO) << "Coordinator starts to sleep " << timeToRun
              << " milliseconds.";
    std::this_thread::sleep_for(std::chrono::milliseconds(timeToRun));
    stopFlag.store(true);
    LOG(INFO) << "Coordinator awakes.";

    uint64_t totalTransaction = 0;

    for (auto i = 0u; i < context.workerNum; i++) {
      threads[i].join();
      totalTransaction += workers[i]->transactionId + 1;
    }
    epochThread.join();

    LOG(INFO) << "Coordinator executed " << totalTransaction
              << " transactions in " << timeToRun << " milliseconds.";

    LOG(INFO) << "Coordinator exits.";
  }

private:
  void advanceEpoch() {

    LOG(INFO) << "Coordinator epoch thread starts.";

    auto sleepTime = std::chrono::milliseconds(40);

    while (!stopFlag.load()) {
      std::this_thread::sleep_for(sleepTime);
      epoch.fetch_add(1);
    }

    LOG(INFO) << "Coordinator epoch thread exits, last epoch = " << epoch.load() << ".";
  }

private:
  std::size_t id;
  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;
  DatabaseType &db;
  ContextType &context;

  std::vector<std::unique_ptr<Worker<WorkloadType>>> workers;
};
} // namespace scar
