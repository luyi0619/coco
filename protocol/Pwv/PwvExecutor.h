//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Bohm/Bohm.h"
#include "protocol/Bohm/BohmHelper.h"
#include "protocol/Bohm/BohmMessage.h"
#include "protocol/Bohm/BohmPartitioner.h"

#include <chrono>
#include <thread>

namespace scar {

template <class Workload> class PwvExecutor : public Worker {
public:
  using WorkloadType = Workload;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using StorageType = typename WorkloadType::StorageType;
  using TransactionType = BohmTransaction;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using ProtocolType = Bohm<DatabaseType>;

  PwvExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
              const ContextType &context,
              std::vector<std::unique_ptr<TransactionType>> &transactions,
              std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
              std::atomic<uint32_t> &worker_status,
              std::atomic<uint32_t> &n_complete_workers,
              std::atomic<uint32_t> &n_started_workers)
      : Worker(coordinator_id, id), db(db), context(context),
        transactions(transactions), storages(storages), epoch(epoch),
        worker_status(worker_status), n_complete_workers(n_complete_workers),
        n_started_workers(n_started_workers),
        partitioner(coordinator_id, context.coordinator_num),
        workload(coordinator_id, db, random, partitioner),
        init_transaction(false),
        random(id), // make sure each worker has a different seed.
        sleep_random(reinterpret_cast<uint64_t>(this)) {}

  ~PwvExecutor() = default;

  void start() override {

    LOG(INFO) << "PwvExecutor" << id << " started. ";

    for (;;) {

      ExecutorStatus status;
      do {
        status = static_cast<ExecutorStatus>(worker_status.load());

        if (status == ExecutorStatus::EXIT) {
          LOG(INFO) << "PwvExecutor" << id << " exits. ";
          return;
        }
      } while (status != ExecutorStatus::Pwv_Analysis);

      n_started_workers.fetch_add(1);
      generate_transactions();
      n_complete_workers.fetch_add(1);
      // wait to Execute
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Pwv_Analysis) {
        std::this_thread::yield();
      }

      n_started_workers.fetch_add(1);
      run_transactions();
      n_complete_workers.fetch_add(1);
      // wait to execute
      while (static_cast<ExecutorStatus>(worker_status.load()) ==
             ExecutorStatus::Pwv_Execute) {
        std::this_thread::yield();
      }
    }
  }

  void onExit() override {
    LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50)
              << " us (50%) " << percentile.nth(75) << " us (75%) "
              << percentile.nth(95) << " us (95%) " << percentile.nth(99)
              << " us (99%).";
  }

  void generate_transactions() {
    uint32_t cur_epoch = epoch.load();
    for (auto i = id; i < transactions.size(); i += context.worker_num) {
      if (!context.same_batch || !init_transaction) {
        // generate transaction
        auto partition_id = random.uniform_dist(0, context.partition_num - 1);
        transactions[i] =
            workload.next_transaction(context, partition_id, storages[i]);
        prepare_transaction(*transactions[i]);
      } else {
        // a soft reset
        transactions[i]->network_size = 0;
        transactions[i]->load_read_count();
        transactions[i]->clear_execution_bit();
      }
      transactions[i]->set_id(epoch, i);
    }
    init_transaction = true;
  }

  void prepare_transaction(TransactionType &txn) {
    // run execute to prepare read/write set
    auto result = txn.execute(id);
    if (result == TransactionResult::ABORT_NORETRY) {
      txn.abort_no_retry = true;
      n_abort_no_retry.fetch_add(1);
    }
  }

  void run_transactions() {

    auto run_transaction = [this](TransactionType *transaction) {
      if (transaction->abort_no_retry) {
        return;
      }

      // spin until the reads are ready
      for (;;) {
        auto result = transaction->execute(id);
        n_network_size += transaction->network_size;
        if (result == TransactionResult::READY_TO_COMMIT) {
          protocol.commit(*transaction);
          n_commit.fetch_add(1);
          auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - transaction->startTime)
                  .count();
          percentile.add(latency);
          break;
        } else if (result == TransactionResult::ABORT) {
          protocol.abort(*transaction);
          n_abort_lock.fetch_add(1);
          if (context.sleep_on_retry) {
            std::this_thread::sleep_for(std::chrono::microseconds(
                sleep_random.uniform_dist(0, context.sleep_time)));
          }
        } else {
          CHECK(false)
              << "abort no retry transactions should not be scheduled.";
        }
      }
    };

    if (context.bohm_local) {
      for (auto i = id; i < transactions.size(); i += context.worker_num) {
        if (!partitioner.has_master_partition(transactions[i]->partition_id)) {
          continue;
        }
        run_transaction(transactions[i].get());
      }
    } else {
      for (auto i = id + coordinator_id * context.worker_num;
           i < transactions.size();
           i += context.worker_num * context.coordinator_num) {
        run_transaction(transactions[i].get());
      }
    }
  }

private:
  DatabaseType &db;
  const ContextType &context;
  std::vector<std::unique_ptr<TransactionType>> &transactions;
  std::vector<StorageType> &storages;
  std::atomic<uint32_t> &epoch, &worker_status;
  std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
  BohmPartitioner partitioner;
  WorkloadType workload;
  bool init_transaction;
  RandomType random, sleep_random;
  ProtocolType protocol;
  Percentile<int64_t> percentile;
};
} // namespace scar