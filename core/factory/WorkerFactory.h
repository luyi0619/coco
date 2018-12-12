//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"

#include "benchmark/retwis/Workload.h"
#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"

#include "protocol/Scar/Scar.h"
#include "protocol/Scar/ScarExecutor.h"
#include "protocol/ScarSI/ScarSI.h"
#include "protocol/ScarSI/ScarSIExecutor.h"
#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include "protocol/TwoPL/TwoPL.h"
#include "protocol/TwoPL/TwoPLExecutor.h"

#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/SiloGC/SiloGC.h"
#include "protocol/SiloGC/SiloGCExecutor.h"
#include "protocol/SiloRC/SiloRC.h"
#include "protocol/SiloRC/SiloRCExecutor.h"
#include "protocol/TwoPLGC/TwoPLGC.h"
#include "protocol/TwoPLGC/TwoPLGCExecutor.h"

#include "protocol/RStore/RStore.h"
#include "protocol/RStore/RStoreExecutor.h"
#include "protocol/RStore/RStoreManager.h"
#include "protocol/RStoreNC/RStoreNCExecutor.h"
#include "protocol/RStoreNC/RStoreNCManager.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinManager.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include <protocol/Calvin/CalvinTransaction.h>
#include <unordered_set>

namespace scar {

template <class Context> class InferType {};

template <> class InferType<scar::tpcc::Context> {
public:
  template <class Transaction>
  using WorkloadType = scar::tpcc::Workload<Transaction>;
};

template <> class InferType<scar::ycsb::Context> {
public:
  template <class Transaction>
  using WorkloadType = scar::ycsb::Workload<Transaction>;
};

template <> class InferType<scar::retwis::Context> {
public:
  template <class Transaction>
  using WorkloadType = scar::retwis::Workload<Transaction>;
};

class WorkerFactory {

public:
  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db,
                 const Context &context, std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {
        "Silo",   "SiloGC",   "SiloRC", "Scar",    "ScarSI",
        "RStore", "RStoreNC", "TwoPL",  "TwoPLGC", "Calvin"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Silo") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

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
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloGCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "SiloRC") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloRCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "Scar") {

      using TransactionType = scar::ScarTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<ScarExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "ScarSI") {

      using TransactionType = scar::ScarTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<ScarSIExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    }

    else if (context.protocol == "RStore") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<RStoreManager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<RStoreExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "RStoreNC") {

      using TransactionType = scar::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<RStoreNCManager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<RStoreNCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "TwoPL") {

      using TransactionType = scar::TwoPLTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

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
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

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
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<CalvinManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      std::vector<CalvinExecutor<WorkloadType> *> all_executors;

      for (auto i = 0u; i < context.worker_num; i++) {

        auto w = std::make_shared<CalvinExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->lock_manager_status,
            manager->worker_status, manager->n_completed_workers,
            manager->n_started_workers);
        workers.push_back(w);
        manager->add_worker(w);
        all_executors.push_back(w.get());
      }
      // push manager to workers
      workers.push_back(manager);

      for (auto i = 0u; i < context.worker_num; i++) {
        static_cast<CalvinExecutor<WorkloadType> *>(workers[i].get())
            ->set_all_executors(all_executors);
      }
    }

    return workers;
  }
};
} // namespace scar