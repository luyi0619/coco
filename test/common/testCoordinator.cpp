//
// Created by Yi Lu on 8/28/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Silo.h"
#include <gtest/gtest.h>

TEST(TestCoordinator, TestTPCC) {

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<ProtocolType>;

  scar::tpcc::Context context;
  context.partitionNum = 4;
  context.workerNum = 4;
  scar::tpcc::Random random;

  scar::tpcc::Database<MetaDataType> db;
  db.initialize(context, 4, 4);

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;

  scar::Coordinator<WorkloadType> c(0, db, context);
  c.start();

  EXPECT_EQ(true, true);
}