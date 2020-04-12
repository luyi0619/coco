//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Manager.h"
#include "core/Partitioner.h"
#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaTransaction.h"

#include <atomic>
#include <thread>
#include <vector>

namespace scar {

template <class Workload> class AriaManager : public scar::Manager {
public:
  using base_type = scar::Manager;

  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;

  using TransactionType = AriaTransaction;
  static_assert(std::is_same<typename WorkloadType::TransactionType,
                             TransactionType>::value,
                "Transaction types do not match.");
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  AriaManager(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context, std::atomic<bool> &stopFlag)
      : base_type(coordinator_id, id, context, stopFlag), db(db), epoch(0) {

    storages.resize(context.batch_size);
    transactions.resize(context.batch_size);
  }

  void coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    while (!stopFlag.load()) {

      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      // then, each worker threads generates a transaction using the same seed.
      epoch.fetch_add(1);

      // LOG(INFO) << "Seed: " << random.get_seed();
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Aria_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      // wait for all machines until they finish the Aria_READ phase.
      wait4_ack();

      // Allow each worker to commit transactions
      n_started_workers.store(0);
      n_completed_workers.store(0);
      signal_worker(ExecutorStatus::Aria_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();

      // clean batch now
      cleanup_batch();
      // wait for all machines until they finish the Aria_COMMIT phase.
      // this can be skipped
      // wait4_ack();
      wait4_commit_tids_from_non_master();
      broadcast_commit_tids();

      /*
            // prepare transactions for calvin and clear the metadata
            cleanup_batch();
            n_started_workers.store(0);
            n_completed_workers.store(0);
            signal_worker(ExecutorStatus::Aria_Fallback_Prepare);
            wait_all_workers_start();
            wait_all_workers_finish();
            broadcast_stop();
            wait4_stop(n_coordinators - 1);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::STOP);
            wait_all_workers_finish();
            // wait for all machines until they finish the Aria_COMMIT phase.
            wait4_ack();

            // calvin execution
            n_started_workers.store(0);
            n_completed_workers.store(0);
            signal_worker(ExecutorStatus::Aria_Fallback);
            wait_all_workers_start();
            wait_all_workers_finish();
            broadcast_stop();
            wait4_stop(n_coordinators - 1);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::STOP);
            wait_all_workers_finish();
            // wait for all machines until they finish the Aria_COMMIT phase.
            wait4_ack();
      */
    }
    signal_worker(ExecutorStatus::EXIT);
  }

  void non_coordinator_start() override {

    std::size_t n_workers = context.worker_num;
    std::size_t n_coordinators = context.coordinator_num;

    for (;;) {
      // LOG(INFO) << "Seed: " << random.get_seed();
      ExecutorStatus status = wait4_signal();
      if (status == ExecutorStatus::EXIT) {
        set_worker_status(ExecutorStatus::EXIT);
        break;
      }

      DCHECK(status == ExecutorStatus::Aria_READ);
      // the coordinator on each machine first moves the aborted transactions
      // from the last batch earlier to the next batch and set remaining
      // transaction slots to null.

      epoch.fetch_add(1);

      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Aria_READ);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();
      send_ack();

      status = wait4_signal();
      DCHECK(status == ExecutorStatus::Aria_COMMIT);
      n_started_workers.store(0);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::Aria_COMMIT);
      wait_all_workers_start();
      wait_all_workers_finish();
      broadcast_stop();
      wait4_stop(n_coordinators - 1);
      n_completed_workers.store(0);
      set_worker_status(ExecutorStatus::STOP);
      wait_all_workers_finish();

      // clean batch now
      cleanup_batch();
      // this can be skipped
      // send_ack();
      send_commit_tids_to_master();
      wait4_commit_tids_from_master();

      /*
            status = wait4_signal();
            DCHECK(status == ExecutorStatus::Aria_Fallback_Prepare);
            cleanup_batch();
            n_started_workers.store(0);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::Aria_Fallback_Prepare);
            wait_all_workers_start();
            wait_all_workers_finish();
            broadcast_stop();
            wait4_stop(n_coordinators - 1);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::STOP);
            wait_all_workers_finish();
            send_ack();

            status = wait4_signal();
            DCHECK(status == ExecutorStatus::Aria_Fallback);
            n_started_workers.store(0);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::Aria_Fallback);
            wait_all_workers_start();
            wait_all_workers_finish();
            broadcast_stop();
            wait4_stop(n_coordinators - 1);
            n_completed_workers.store(0);
            set_worker_status(ExecutorStatus::STOP);
            wait_all_workers_finish();
            send_ack();
          */
    }
  }

  /*
   * Assume there are 2 nodes and each node has 3 threads.
   * Node A runs, 0, 2, 4, 6, 8, 10
   * Node B runs, 1, 3, 5, 7, 9, 11
   */

  void cleanup_batch() {
    commit_tids.clear();
    for (auto i = coordinator_id; i < transactions.size();
         i += context.coordinator_num) {
      if (transactions[i]->abort_lock || transactions[i]->abort_no_retry)
        continue;
      commit_tids.push_back(transactions[i]->id);
    }
  }

  // the following two functions are called on master

  void wait4_commit_tids_from_non_master() {
    CHECK(coordinator_id == 0);

    std::size_t n_coordinators = context.coordinator_num;

    for (auto i = 0u; i < n_coordinators - 1; i++) {
      vector_in_queue.wait_till_non_empty();
      std::unique_ptr<Message> message(vector_in_queue.front());
      bool ok = vector_in_queue.pop();
      CHECK(ok);
      CHECK(message->get_message_count() == 1);

      MessagePiece messagePiece = *(message->begin());
      auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
      CHECK(type == ControlMessage::VECTOR);

      decltype(commit_tids.size()) sz;
      StringPiece stringPiece = messagePiece.toStringPiece();
      Decoder dec(stringPiece);
      dec >> sz;
      for (auto k = 0u; k < sz; k++) {
        int val;
        dec >> val;
        commit_tids.push_back(val);
      }
    }
  }

  void broadcast_commit_tids() {
    CHECK(coordinator_id == 0);
    std::size_t n_coordinators = context.coordinator_num;
    for (auto i = 0u; i < n_coordinators; i++) {
      if (i == coordinator_id)
        continue;
      ControlMessageFactory::new_vector_message(*messages[i], commit_tids);
      flush_messages();
    }
  }

  // the following two functions are called on non-master
  void send_commit_tids_to_master() {
    CHECK(coordinator_id != 0);

    ControlMessageFactory::new_vector_message(*messages[0], commit_tids);
    flush_messages();
  }

  void wait4_commit_tids_from_master() {
    CHECK(coordinator_id != 0);
    std::size_t n_coordinators = context.coordinator_num;

    vector_in_queue.wait_till_non_empty();
    std::unique_ptr<Message> message(vector_in_queue.front());
    bool ok = vector_in_queue.pop();
    CHECK(ok);
    CHECK(message->get_message_count() == 1);

    MessagePiece messagePiece = *(message->begin());
    auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
    CHECK(type == ControlMessage::VECTOR);

    decltype(commit_tids.size()) sz;
    commit_tids.clear();
    StringPiece stringPiece = messagePiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> sz;
    for (auto k = 0u; k < sz; k++) {
      int val;
      dec >> val;
      commit_tids.push_back(val);
    }
  }

public:
  RandomType random;
  DatabaseType &db;
  std::atomic<uint32_t> epoch;
  std::vector<StorageType> storages;
  std::vector<std::unique_ptr<TransactionType>> transactions;
  std::atomic<uint32_t> total_abort;
  std::vector<int> commit_tids;
};
} // namespace scar