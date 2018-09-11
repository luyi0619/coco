//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Executor.h"
#include "core/Manager.h"
#include "protocol/Silo/Silo.h"

#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/SiloGC/SiloGC.h"

#include "protocol/RStore/RStore.h"
#include "protocol/RStore/RStoreExecutor.h"
#include "protocol/RStore/RStoreManager.h"

#include <unordered_set>

namespace scar {

class WorkerFactory {

public:
  template <class Workload, class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db, Context &context,
                 std::atomic<bool> &stop_flag) {

    std::unordered_set<std::string> protocols = {"Silo", "SiloGC", "RStore"};
    CHECK(protocols.count(context.protocol) == 1);

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Silo") {

      auto manager = std::make_shared<Manager<Workload, Silo<Database>>>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<Executor<Workload, Silo<Database>>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }

      workers.push_back(manager);

    } else if (context.protocol == "SiloGC") {

      auto manager =
          std::make_shared<group_commit::Manager<Workload, SiloGC<Database>>>(
              coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<
                          group_commit::Executor<Workload, SiloGC<Database>>>(
            coordinator_id, i, db, context, manager->worker_status,
            manager->n_completed_workers, manager->n_started_workers));
      }
      workers.push_back(manager);

    } else if (context.protocol == "RStore") {

      auto switcher = std::make_shared<RStoreManager<Workload>>(
          coordinator_id, context.worker_num, context, stop_flag);

      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<RStoreExecutor<Workload>>(
            coordinator_id, i, db, context, switcher->worker_status,
            switcher->n_completed_workers, switcher->n_started_workers));
      }
      workers.push_back(switcher);
    }

    return workers;
  }
};
} // namespace scar