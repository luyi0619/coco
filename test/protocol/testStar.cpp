//
// Created by Yi Lu on 9/6/18.
//
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "protocol/Star/Star.h"
#include "protocol/Star/StarExecutor.h"
#include <gtest/gtest.h>

TEST(TestStar, TestStarSwitcher) {

  using TransactionType = coco::SiloTransaction;
  using WorkloadType = coco::tpcc::Workload<TransactionType>;

  coco::tpcc::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.protocol = "Star";
  coco::tpcc::Random random;

  coco::tpcc::Database db;

  std::atomic<bool> stopFlag;

  coco::StarManager manager(0, 0, context, stopFlag);
  coco::StarExecutor<WorkloadType> e(
      0, 0, db, context, manager.batch_size, manager.worker_status,
      manager.n_completed_workers, manager.n_started_workers);
}
