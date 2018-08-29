//
// Created by Yi Lu on 8/28/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Silo.h"
#include <gflags/gflags.h>
#include <gtest/gtest.h>

DEFINE_int32(threads, 1, "the number of threads.");

TEST(TestCoordinator, TestTPCC) {

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<ProtocolType>;

  int n = FLAGS_threads;

  scar::tpcc::Context context;
  context.partitionNum = n;
  context.workerNum = n;

  scar::tpcc::Database<MetaDataType> db;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;

  scar::Coordinator<WorkloadType> c(0, db, context);

  EXPECT_EQ(true, true);
}