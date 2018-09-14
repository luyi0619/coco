//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "benchmark/tpcc/Workload.h"

#include "core/Executor.h"
#include "core/Manager.h"
#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include "protocol/TwoPL/TwoPL.h"
#include "protocol/TwoPL/TwoPLExecutor.h"

#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/SiloGC/SiloGC.h"
#include "protocol/SiloGC/SiloGCExecutor.h"
#include "protocol/TwoPLGC/TwoPLGC.h"
#include "protocol/TwoPLGC/TwoPLGCExecutor.h"

#include "protocol/RStore/RStore.h"
#include "protocol/RStore/RStoreExecutor.h"
#include "protocol/RStore/RStoreManager.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinLockManager.h"
#include "protocol/Calvin/CalvinManager.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include <protocol/Calvin/CalvinTransaction.h>
#include <unordered_set>

namespace scar {

class WorkerFactory {

public:
  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db, Context &context,
                 std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {"Silo",  "SiloGC",  "RStore",
                                                 "TwoPL", "TwoPLGC", "Calvin"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Silo") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType = scar::tpcc::Workload<TransactionType>;

      auto manager = std::make_shared<Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "SiloGC") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType = scar::tpcc::Workload<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloGCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "RStore") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType = scar::tpcc::Workload<TransactionType>;

      auto manager = std::make_shared<RStoreManager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<RStoreExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);
    } else if (context.protocol == "TwoPL") {

      using TransactionType = scar::TwoPLTransaction;
      using WorkloadType = scar::tpcc::Workload<TransactionType>;

      auto manager = std::make_shared<Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<TwoPLExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "TwoPLGC") {

      using TransactionType = scar::TwoPLTransaction;
      using WorkloadType = scar::tpcc::Workload<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<TwoPLGCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);
    } else if (context.protocol == "Calvin") {

      using TransactionType = scar::CalvinTransaction;
      using WorkloadType = scar::tpcc::Workload<TransactionType>;

      // create manager

      auto manager = std::make_shared<CalvinManager>(
          coordinator_id, context.worker_num, context, stop_flag);

      // create lock manager

      std::vector<std::shared_ptr<CalvinLockManager<WorkloadType>>>
          lock_managers;

      for (auto i = 0u; i < context.lock_manager_num; i++) {
        lock_managers.push_back(
            std::make_shared<CalvinLockManager<WorkloadType>>(
                coordinator_id, context.worker_num + i, i,
                manager->worker_status, manager->n_completed_workers,
                manager->n_started_workers));
      }

      // create worker

      DCHECK(context.worker_num % context.lock_manager_num == 0);

      auto worker_num_per_lock_manager =
          context.worker_num / context.lock_manager_num;

      for (auto i = 0u; i < context.worker_num; i++) {

        auto w = std::make_shared<CalvinExecutor<WorkloadType>>(
            coordinator_id, i, db, context,
            lock_managers[i / worker_num_per_lock_manager]->stop_flag);
        workers.push_back(w);
        lock_managers[i / worker_num_per_lock_manager]->add_worker(w);
      }

      // push lock managers to workers

      for (auto i = 0u; i < lock_managers.size(); i++) {
        workers.push_back(lock_managers[i]);
      }

      workers.push_back(manager);

      DCHECK(workers.size() ==
             context.worker_num + context.lock_manager_num + 1);
    }

    return workers;
  }
};
} // namespace scar