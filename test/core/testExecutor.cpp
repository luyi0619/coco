//
// Created by Yi Lu on 7/24/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Workload.h"
#include "core/Executor.h"
#include "core/Manager.h"
#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include "protocol/Silo/SiloTransaction.h"
#include <gtest/gtest.h>

TEST(TestExecutor, TestTPCC) {

  using MetaDataType = std::atomic<uint64_t>;
  using TransactionType = scar::SiloTransaction;
  using WorkloadType = scar::tpcc::Workload<TransactionType>;

  scar::tpcc::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  scar::tpcc::Random random;

  scar::tpcc::Database<MetaDataType> db;

  std::atomic<bool> stopFlag;
  scar::Manager manager(0, 0, context, stopFlag);
  scar::SiloExecutor<WorkloadType> w(0, 0, db, context, manager.worker_status,
                                     manager.n_completed_workers,
                                     manager.n_started_workers);

  EXPECT_EQ(true, true);
}

TEST(TestWorker, TestYCSB) {

  using MetaDataType = std::atomic<uint64_t>;
  using TransactionType = scar::SiloTransaction;
  using WorkloadType = scar::ycsb::Workload<TransactionType>;

  scar::ycsb::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  scar::ycsb::Random random;

  scar::ycsb::Database<MetaDataType> db;

  std::atomic<bool> stopFlag;
  scar::Manager manager(0, 0, context, stopFlag);
  scar::SiloExecutor<WorkloadType> w(0, 0, db, context, manager.worker_status,
                                     manager.n_completed_workers,
                                     manager.n_started_workers);

  EXPECT_EQ(true, true);
}