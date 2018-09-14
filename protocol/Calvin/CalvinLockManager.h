//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Worker.h"
#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinMessage.h"
#include <glog/logging.h>
#include <thread>

namespace scar {

template <class Workload> class CalvinLockManager : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TableType = typename DatabaseType::TableType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  using ProtocolType = Calvin<DatabaseType>;

  using MessageType = CalvinMessage;
  using MessageFactoryType = CalvinMessageFactory;
  using MessageHandlerType = CalvinMessageHandler;

  CalvinLockManager(std::size_t coordinator_id, std::size_t id,
                    std::size_t shard_id, std::atomic<uint32_t> &worker_status,
                    std::atomic<uint32_t> &n_complete_workers,
                    std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), shard_id(shard_id),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers) {
    stop_flag.store(false);
  }

  ~CalvinLockManager() = default;

  void start() override {
    LOG(INFO) << "CalvinLockManager " << shard_id << " (worker id " << id
              << " ) started, ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "Executor " << id << " exits.";
          return;
        }
      } while (status != ExecutorStatus::START);

      n_started_workers.fetch_add(1);

      // for debug only
      std::this_thread::sleep_for(std::chrono::seconds(1));

      n_complete_workers.fetch_add(1);

      // make sure all lock manager stopped

      while (static_cast<ExecutorStatus>(worker_status.load()) !=
             ExecutorStatus::STOP) {
        std::this_thread::yield();
      }

      n_complete_workers.fetch_add(1);
    }
  }

  void onExit() override {}

  void push_message(Message *message) override { CHECK(false); }

  Message *pop_message() override { return nullptr; }

  void add_worker(const std::shared_ptr<CalvinExecutor<WorkloadType>> &w) {
    workers.push_back(w);
  }

public:
  std::size_t shard_id;
  std::vector<std::shared_ptr<CalvinExecutor<WorkloadType>>> workers;
  std::atomic<bool> stop_flag;
  std::atomic<uint32_t> &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
};

} // namespace scar