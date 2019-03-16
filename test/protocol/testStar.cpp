//
// Created by Yi Lu on 9/6/18.
//
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "protocol/Star/Star.h"
#include "protocol/Star/StarExecutor.h"
#include <gtest/gtest.h>

TEST(TestStar, TestStarSwitcher) {

  using TransactionType = scar::SiloTransaction;
  using WorkloadType = scar::tpcc::Workload<TransactionType>;

  scar::tpcc::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.protocol = "Star";
  scar::tpcc::Random random;

  scar::tpcc::Database db;

  std::atomic<bool> stopFlag;

  scar::StarManager manager(0, 0, context, stopFlag);
  scar::StarExecutor<WorkloadType> e(0, 0, db, context, manager.worker_status,
                                     manager.n_completed_workers,
                                     manager.n_started_workers);
}
