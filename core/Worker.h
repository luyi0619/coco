//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "core/Transaction.h"
#include <atomic>
#include <glog/logging.h>
#include <queue>

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
      : id(id), db(db), context(context), epoch(epoch), stopFlag(stopFlag),
        protocol(db, epoch), workload(db, context, random, protocol) {
    transactionId.store(0);
  }

  void start() {

    std::queue<std::unique_ptr<Transaction<ProtocolType>>> q;

    while (!stopFlag.load()) {
      commitTransactions(q);

      std::unique_ptr<Transaction<ProtocolType>> txn =
          workload.nextTransaction();
      txn->execute();
      transactionId.fetch_add(1);
      q.push(std::move(txn));
    }

    commitTransactions(q, true);
    LOG(INFO) << "Worker " << id << " exits.";
  }

private:
  void
  commitTransactions(std::queue<std::unique_ptr<Transaction<ProtocolType>>> &q,
                     bool retry = false) {

    do {
      auto currentEpoch = epoch.load();
      auto now = std::chrono::steady_clock::now();
      while (!q.empty()) {
        const auto &ptr = q.front();
        if (ptr->commitEpoch < currentEpoch) {
          LOG(INFO) << "Worker " << id << " executes transaction in "
                    << std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - ptr->startTime)
                           .count()
                    << " ms. currentEpoch " << currentEpoch
                    << " , commit epoch " << ptr->commitEpoch;
          q.pop();
        } else {
          break;
        }
      }
    } while (!q.empty() && retry);
  }

public:
  std::atomic<uint64_t> transactionId;

private:
  std::size_t id;
  DatabaseType &db;
  ContextType &context;
  std::atomic<uint64_t> &epoch;
  std::atomic<bool> &stopFlag;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
};

} // namespace scar
