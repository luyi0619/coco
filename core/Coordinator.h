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
      : id(id), db(db), context(context) {}

  void start() {

    LOG(INFO) << "Coordinator initializes " << context.workerNum << " workers.";

    std::atomic<uint64_t> epoch;
    std::atomic<bool> stopFlag;

    for (auto i = 0u; i < context.workerNum; i++) {
      workers.push_back(std::make_unique<Worker<WorkloadType>>(
          i, db, context, epoch, stopFlag));
    }

    std::vector<std::thread> threads;

    LOG(INFO) << "Coordinator starts to run " << context.workerNum
              << " workers.";

    for (auto i = 0u; i < context.workerNum; i++) {
      threads.emplace_back(&Worker<WorkloadType>::start, workers[i].get());
    }

    for (auto i = 0u; i < context.workerNum; i++) {
      threads[i].join();
    }

    LOG(INFO) << "Coordinator exits.";
  }

private:
  std::size_t id;
  DatabaseType &db;
  ContextType &context;

  std::vector<std::unique_ptr<Worker<WorkloadType>>> workers;
};
} // namespace scar
