//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "core/Transaction.h"
#include <atomic>
#include <glog/logging.h>

namespace scar {

template <class Workload> class Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using ProtocolType = typename WorkloadType::ProtocolType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  Worker(std::size_t id, DatabaseType &db, ContextType &context,
         std::atomic<uint64_t> &epoch, std::atomic<bool> &stopFlag)
      : id(id), db(db), context(context), stopFlag(stopFlag),
        protocol(db, epoch), workload(db, context, random, protocol) {
    transactionId.store(0);
  }

  void start() {

    while (!stopFlag.load()) {
      std::unique_ptr<Transaction<ProtocolType>> p = workload.nextTransaction();
      p->execute();
      transactionId.fetch_add(1);

      //      auto now = std::chrono::steady_clock::now();
      //      LOG(INFO) << "Worker " << id << " executes transaction "
      //                << transactionId++ << " in "
      //                <<
      //                std::chrono::duration_cast<std::chrono::microseconds>(
      //                       now - p->startTime)
      //                       .count()
      //                << " ms.";
    }

    LOG(INFO) << "Worker " << id << " exits.";
  }

public:
  std::atomic<uint64_t> transactionId;

private:
  std::size_t id;
  DatabaseType &db;
  ContextType &context;
  std::atomic<bool> &stopFlag;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
};

} // namespace scar
