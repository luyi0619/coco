//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/tpcc/Database.h"
#include <gtest/gtest.h>

TEST(TestTPCCDatabase, TestBasic) {

  using DataT = std::atomic<uint64_t>;

  scar::tpcc::Context context;

  scar::tpcc::Database<DataT> db;
  db.initialize(context, 4, 4);
  EXPECT_EQ(true, true);
}
