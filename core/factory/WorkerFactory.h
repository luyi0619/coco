//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"

#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"

#include "protocol/Scar/Scar.h"
#include "protocol/Scar/ScarExecutor.h"
#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include "protocol/TwoPL/TwoPL.h"
#include "protocol/TwoPL/TwoPLExecutor.h"

#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/ScarGC/ScarGC.h"
#include "protocol/ScarGC/ScarGCExecutor.h"
#include "protocol/ScarSI/ScarSI.h"
#include "protocol/ScarSI/ScarSIExecutor.h"
#include "protocol/SiloGC/SiloGC.h"
#include "protocol/SiloGC/SiloGCExecutor.h"
#include "protocol/SiloSI/SiloSI.h"
#include "protocol/SiloSI/SiloSIExecutor.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaManager.h"
#include "protocol/Aria/AriaTransaction.h"

#include <unordered_set>

namespace coco {

template <class Context> class InferType {};

template <> class InferType<coco::tpcc::Context> {
public:
  template <class Transaction>
  using WorkloadType = coco::tpcc::Workload<Transaction>;
};

template <> class InferType<coco::ycsb::Context> {
public:
  template <class Transaction>
  using WorkloadType = coco::ycsb::Workload<Transaction>;
};

class WorkerFactory {

public:
  template <class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db,
                 const Context &context, std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {"Silo",  "SiloGC", "SiloSI",
                                                 "Scar",  "ScarGC", "ScarSI",
                                                 "TwoPL", "Aria"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Silo") {

      using TransactionType = coco::SiloTransaction;
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

      using TransactionType = coco::SiloTransaction;
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
    } else if (context.protocol == "SiloSI") {

      using TransactionType = coco::SiloTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<SiloSIExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "Scar") {

      using TransactionType = coco::ScarTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<ScarExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "ScarGC") {

      using TransactionType = coco::ScarTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      auto manager = std::make_shared<group_commit::Manager>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<ScarGCExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "ScarSI") {

      using TransactionType = coco::ScarTransaction;
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

    } else if (context.protocol == "TwoPL") {

      using TransactionType = coco::TwoPLTransaction;
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
    } else if (context.protocol == "Aria") {

      using TransactionType = coco::AriaTransaction;
      using WorkloadType =
          typename InferType<Context>::template WorkloadType<TransactionType>;

      // create manager

      auto manager = std::make_shared<AriaManager<WorkloadType>>(
          coordinator_id, context.worker_num, db, context, stop_flag);

      // create worker

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<AriaExecutor<WorkloadType>>(
            coordinator_id, i, db, context, manager->transactions,
            manager->storages, manager->epoch, manager->worker_status,
            manager->total_abort, manager->n_completed_workers,
            manager->n_started_workers));
      }

      workers.push_back(manager);
    } else {
      CHECK(false) << "protocol: " << context.protocol << " is not supported.";
    }

    return workers;
  }
};
} // namespace coco