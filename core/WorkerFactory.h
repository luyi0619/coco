//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Executor.h"
#include "protocol/RStore/RStoreExecutor.h"
#include "protocol/RStore/RStoreSwitcher.h"
#include "protocol/Silo/Silo.h"

namespace scar {

class WorkerFactory {

public:
  template <class Workload, class Database, class Context>
  static std::vector<std::shared_ptr<Worker>>
  create_workers(std::size_t coordinator_id, Database &db, Context &context,
                 std::atomic<bool> &stop_flag) {

    std::vector<std::shared_ptr<Worker>> workers;

    if (context.protocol == "Silo") {
      for (auto i = 0u; i < context.workerNum; i++) {
        workers.push_back(std::make_shared<Executor<Workload, Silo<Database>>>(
            coordinator_id, i, db, context, stop_flag));
      }
    } else if (context.protocol == "RStore") {

      std::vector<std::shared_ptr<Worker>> workers;
      std::shared_ptr<RStoreSwitcher<Workload>> switcher =
          std::make_shared<RStoreSwitcher<Workload>>(
              coordinator_id, context.workerNum, context, stop_flag);

      for (auto i = 0u; i < context.workerNum; i++) {
        workers.push_back(std::make_shared<RStoreExecutor<Workload>>(
            coordinator_id, i, db, context, switcher->worker_status,
            switcher->n_completed_workers));
      }
      workers.push_back(switcher);
    }

    return workers;
  }
};
} // namespace scar