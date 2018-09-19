//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"

namespace scar {

class RStoreManager : public scar::Manager {
public:
  using base_type = scar::Manager;

  RStoreManager(std::size_t coordinator_id, std::size_t id,
                const Context &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag) {}

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    Percentile<int64_t> all_percentile, c_percentile, s_percentile;

    while (!stopFlag.load()) {

      auto c_start = std::chrono::steady_clock::now();
      // start c-phase

      // LOG(INFO) << "start C-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      signal_worker(ExecutorStatus::C_PHASE);
      wait_all_workers_start();
      wait_all_workers_finish();
      set_worker_status(ExecutorStatus::STOP);
      broadcast_stop();
      wait4_ack();

      c_percentile.add(std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - c_start)
                           .count());

      auto s_start = std::chrono::steady_clock::now();
      // start s-phase

      // LOG(INFO) << "start S-Phase";

      n_completed_workers.store(0);
      n_started_workers.store(0);
      signal_worker(ExecutorStatus::S_PHASE);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      wait4_ack();

      auto now = std::chrono::steady_clock::now();

      s_percentile.add(
          std::chrono::duration_cast<std::chrono::microseconds>(now - s_start)
              .count());

      all_percentile.add(
          std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
              .count());
    }

    signal_worker(ExecutorStatus::EXIT);

    LOG(INFO) << "Average phase switch length " << all_percentile.nth(50)
              << " us, average c phase length " << c_percentile.nth(50)
              << " us, average s phase length " << s_percentile.nth(50)
              << " us.";
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {

      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      // LOG(INFO) << "start C-Phase";

      // start c-phase

      DCHECK(status == ExecutorStatus::C_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::C_PHASE);
      wait_all_workers_start();
      wait4_stop(1);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      // LOG(INFO) << "start S-Phase";

      // start s-phase

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::S_PHASE);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::S_PHASE);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();
    }
  }
};
} // namespace scar