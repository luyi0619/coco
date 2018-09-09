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
      for (auto i = 0u; i < context.worker_num; i++) {
        workers.push_back(std::make_shared<Executor<Workload, Silo<Database>>>(
            coordinator_id, i, db, context, stop_flag));
      }
    } else if (context.protocol == "RStore") {

      std::shared_ptr<RStoreSwitcher<Workload>> switcher =
          std::make_shared<RStoreSwitcher<Workload>>(
              coordinator_id, context.worker_num, context, stop_flag);

      switcher->worker_status.store(
          static_cast<uint32_t>(RStoreWorkerStatus::STOP));

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