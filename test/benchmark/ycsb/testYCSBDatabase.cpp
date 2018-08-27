//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/ycsb/Database.h"
#include <gtest/gtest.h>

TEST(TestYCSBDatabase, TestBasic) {
  using DataT = std::atomic<uint64_t>;

  scar::ycsb::Context context;
  context.strategy = scar::ycsb::PartitionStrategy::ROUND_ROBIN;
  context.keysPerPartition = 20;
  context.keysPerTransaction = 10;

  scar::ycsb::Database<DataT> db;
  db.initialize(context, 4, 4);
  EXPECT_EQ(true, true);
}
