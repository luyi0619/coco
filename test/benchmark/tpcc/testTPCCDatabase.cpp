//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/tpcc/Database.h"
#include <gtest/gtest.h>

TEST(TestTPCCDatabase, TestBasic) {

  scar::tpcc::Context context;
  context.coordinator_num = 1;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  scar::tpcc::Database db;
  db.initialize(context);
  EXPECT_EQ(true, true);
}
