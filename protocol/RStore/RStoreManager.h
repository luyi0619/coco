//
// Created by Yi Lu on 9/6/18.
//

#pragma once

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

    while (!stopFlag.load()) {

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
    }

    signal_worker(ExecutorStatus::EXIT);
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