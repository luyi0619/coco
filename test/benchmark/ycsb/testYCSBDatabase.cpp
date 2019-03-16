//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/ycsb/Database.h"
#include <gtest/gtest.h>

TEST(TestYCSBDatabase, TestBasic) {

  scar::ycsb::Context context;
  context.strategy = scar::ycsb::PartitionStrategy::ROUND_ROBIN;
  context.keysPerPartition = 20;
  context.keysPerTransaction = 10;
  context.partition_num = 4;
  context.worker_num = 4;

  scar::ycsb::Database db;
  db.initialize(context);
  EXPECT_EQ(true, true);
}
