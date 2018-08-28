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
        protocol(db, epoch), workload(db, context, random, protocol) {}

  void start() {

    int cnt = 0;

    while (!stopFlag.load()) {
      std::unique_ptr<Transaction<ProtocolType>> p = workload.nextTransaction();
      p->execute();

      if (++cnt == 10) {
        break;
      }
    }

    LOG(INFO) << "Worker " << id << " exits.";
  }

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
