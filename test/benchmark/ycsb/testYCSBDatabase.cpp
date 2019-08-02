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
  context.coordinator_num = 1;
  context.partitioner = "hash";
  scar::ycsb::Database db;
  db.initialize(context);

  auto ycsb = sizeof(scar::ycsb::ycsb::key) + sizeof(scar::ycsb::ycsb::value);
  auto total_size = 200000 * ycsb;

  LOG(INFO) << "YCSB total size: " << total_size << " bytes.";

  EXPECT_EQ(true, true);
}
