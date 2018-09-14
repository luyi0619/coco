//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Manager.h"

#include <thread>

namespace scar {

/*
 * In the example (see comment at the top) of CalvinPartitioner.h,
 * coordinator 0 would be the real coordinator, it collects acks from
 * coordinator 1 and coordinator 2 in each batch of queries.
 */

class CalvinManager : public scar::Manager {
public:
  using base_type = scar::Manager;

  CalvinManager(std::size_t coordinator_id, std::size_t id,
                const Context &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag) {}

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      n_completed_workers.store(0);
      n_started_workers.store(0);
      signal_worker(ExecutorStatus::START);
      wait_all_lock_managers_start();
      wait_all_lock_managers_finish();
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_lock_managers_finish();
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

      DCHECK(status == ExecutorStatus::START);
      n_completed_workers.store(0);
      n_started_workers.store(0);
      set_worker_status(ExecutorStatus::START);
      wait_all_lock_managers_start();
      wait_all_lock_managers_finish();
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_lock_managers_finish();
      send_ack();
    }
  }

  virtual void wait_all_lock_managers_finish() {
    std::size_t n_lock_manager = context.lock_manager_num;
    // wait for all workers to finish
    while (n_completed_workers.load() < n_lock_manager) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  virtual void wait_all_lock_managers_start() {
    std::size_t n_lock_manager = context.lock_manager_num;
    // wait for all workers to finish
    while (n_started_workers.load() < n_lock_manager) {
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }
};
} // namespace scar