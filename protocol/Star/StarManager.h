//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"

namespace scar {

class StarManager : public scar::Manager {
public:
  using base_type = scar::Manager;

  StarManager(std::size_t coordinator_id, std::size_t id,
              const Context &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag) {}

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    Percentile<int64_t> all_percentile, c_percentile, s_percentile;
    Percentile<double> overhead_percentile;

    while (!stopFlag.load()) {

      int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
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
      wait4_ack(true);

      {
        auto now = std::chrono::steady_clock::now();
        ack_wait_time_c = std::chrono::duration_cast<std::chrono::microseconds>(
                              now - time_point)
                              .count();
        c_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count());
      }

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
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish(true);
      wait4_ack();
      {
        auto now = std::chrono::steady_clock::now();
        ack_wait_time_s = std::chrono::duration_cast<std::chrono::microseconds>(
                              now - time_point)
                              .count();

        s_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - s_start)
                .count());

        all_percentile.add(
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count());

        overhead_percentile.add(
            100.0 * (ack_wait_time_c + ack_wait_time_s) /
            std::chrono::duration_cast<std::chrono::microseconds>(now - c_start)
                .count());
      }
    }

    signal_worker(ExecutorStatus::EXIT);

    LOG(INFO) << "Average phase switch length " << all_percentile.nth(50)
              << " us, average c phase length " << c_percentile.nth(50)
              << " us, average s phase length " << s_percentile.nth(50)
              << " us, average overhead: " << overhead_percentile.nth(50)
              << " %.";
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
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();
    }
  }

  void wait_all_workers_finish(bool reset = false) {
    std::size_t n_workers = context.worker_num;
    // wait for all workers to finish
    std::size_t value = 0;
    while ((value = n_completed_workers.load()) < n_workers) {
      if (reset && value > 0) {
        reset_time_point();
        reset = false;
      }
      // change to nop_pause()?
      std::this_thread::yield();
    }
  }

  void wait4_ack(bool reset = false) {

    std::chrono::steady_clock::time_point start;

    // only coordinator waits for ack
    DCHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {

      ack_in_queue.wait_till_non_empty();

      if (reset) {
        reset_time_point();
        reset = false;
      }

      std::unique_ptr<Message> message(ack_in_queue.front());
      bool ok = ack_in_queue.pop();
      CHECK(ok);

      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    }
  }

  void reset_time_point() { time_point = std::chrono::steady_clock::now(); }

public:
  std::chrono::steady_clock::time_point time_point;
};
} // namespace scar