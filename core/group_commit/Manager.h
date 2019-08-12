//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/FastSleep.h"
#include "core/Manager.h"

namespace scar {
namespace group_commit {

class Manager : public scar::Manager {
public:
  using base_type = scar::Manager;

  Manager(std::size_t coordinator_id, std::size_t id, const Context &context,
          std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag) {}

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    std::chrono::steady_clock::time_point start, end;
    std::size_t group_time = 1000 * context.group_time,
                total_time = 1000 * context.group_time;

    while (!stopFlag.load()) {
      start = std::chrono::steady_clock::now();

      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::START);
      wait_all_workers_start();
      group_time = get_group_time(group_time, total_time);
      std::this_thread::sleep_for(std::chrono::microseconds(group_time));
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      simulate_durable_write();
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::CLEANUP);
      wait_all_workers_finish();
      wait4_ack();

      end = std::chrono::steady_clock::now();
      total_time =
          std::chrono::duration_cast<std::chrono::microseconds>(end - start)
              .count();
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
      wait_all_workers_start();
      wait4_stop(1);
      simulate_durable_write();
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 2);
      // process replication
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::CLEANUP);
      wait_all_workers_finish();
      send_ack();
    }
  }

  // in microseconds.
  std::size_t get_group_time(std::size_t group_time, std::size_t total_time) {
    if (total_time < group_time) {
      total_time = group_time;
    }

    if (context.exact_group_commit) {
      int adjusted_group_time =
          1000 * context.group_time * group_time / total_time;
      if (adjusted_group_time < 1000) {
        adjusted_group_time = 1000;
      }
      return adjusted_group_time;
    } else {
      return group_time;
    }
  }

protected:
  void simulate_durable_write() {
    if (context.durable_write_cost > 0) {
      FastSleep::sleep_for(context.durable_write_cost);
    }
  }
};

} // namespace group_commit
} // namespace scar
