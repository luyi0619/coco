//
// Created by Yi Lu on 7/24/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Workload.h"
#include "core/Manager.h"
#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include <gtest/gtest.h>

TEST(TestExecutor, TestTPCC) {

  using TransactionType = coco::SiloTransaction;
  using WorkloadType = coco::tpcc::Workload<TransactionType>;

  coco::tpcc::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  coco::tpcc::Random random;

  coco::tpcc::Database db;

  std::atomic<bool> stopFlag;
  coco::Manager manager(0, 0, context, stopFlag);
  coco::SiloExecutor<WorkloadType> w(0, 0, db, context, manager.worker_status,
                                     manager.n_completed_workers,
                                     manager.n_started_workers);

  EXPECT_EQ(true, true);
}

TEST(TestWorker, TestYCSB) {

  using TransactionType = coco::SiloTransaction;
  using WorkloadType = coco::ycsb::Workload<TransactionType>;

  coco::ycsb::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  coco::ycsb::Random random;

  coco::ycsb::Database db;

  std::atomic<bool> stopFlag;
  coco::Manager manager(0, 0, context, stopFlag);
  coco::SiloExecutor<WorkloadType> w(0, 0, db, context, manager.worker_status,
                                     manager.n_completed_workers,
                                     manager.n_started_workers);

  EXPECT_EQ(true, true);
}