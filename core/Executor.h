//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/Percentile.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>

namespace scar {

template <class Workload, class Protocol> class Executor : public Worker {
public:
  using WorkloadType = Workload;
  using ProtocolType = Protocol;
  using RWKeyType = typename WorkloadType::RWKeyType;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  Executor(std::size_t id, DatabaseType &db, ContextType &context,
           std::atomic<uint64_t> &epoch, std::atomic<bool> &stopFlag)
      : Worker(id), db(db), context(context), epoch(epoch), stopFlag(stopFlag),
        protocol(db, epoch), workload(db, context, random),
        syncMessage(nullptr), asyncMessage(nullptr) {
    transactionId.store(0);
  }

  void start() override {
    std::queue<std::unique_ptr<TransactionType>> q;

    while (!stopFlag.load()) {
      commitTransactions(q);

      std::unique_ptr<TransactionType> transaction = workload.nextTransaction();
      setupHandlers(transaction.get());

      transaction->execute();
      transactionId.fetch_add(1);
      q.push(std::move(transaction));
    }

    commitTransactions(q, true);
    LOG(INFO) << "Worker " << id << " exits.";
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << "ms (50%) " << percentile.nth(75) << "ms (75%) "
              << percentile.nth(99.9)
              << "ms (99.9%), size: " << percentile.size() * sizeof(int64_t)
              << " bytes.";
  }

  bool process_request() {

    // only support local read request
    // TODO: support remote request

    return true;
  }

private:
  void setupHandlers(TransactionType *transaction) {
    transaction->readRequestHandler =
        [protocol = this->protocol](std::size_t table_id,
                                    std::size_t partition_id, const void *key,
                                    void *value) {
          protocol.search(table_id, partition_id, key, value);
        };

    transaction->remoteRequestHandler = [this]() { return process_request(); };
  }

  void commitTransactions(std::queue<std::unique_ptr<TransactionType>> &q,
                          bool retry = false) {
    using namespace std::chrono;
    do {
      auto currentEpoch = epoch.load();
      auto now = steady_clock::now();
      while (!q.empty()) {
        const auto &ptr = q.front();
        if (ptr->commitEpoch < currentEpoch) {
          auto latency = duration_cast<milliseconds>(now - ptr->startTime);
          percentile.add(latency.count());
          q.pop();
        } else {
          break;
        }
      }
    } while (!q.empty() && retry);
  }

private:
  DatabaseType &db;
  ContextType &context;
  std::atomic<uint64_t> &epoch;
  std::atomic<bool> &stopFlag;
  RandomType random;
  ProtocolType protocol;
  WorkloadType workload;
  Percentile<int64_t> percentile;
  std::unique_ptr<Message> syncMessage, asyncMessage;
};
} // namespace scar