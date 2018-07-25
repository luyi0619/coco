//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/tpcc/Database.h"
#include "protocol/Silo.h"
#include <gtest/gtest.h>

TEST(TestTPCCDatabase, TestBasic) {

  scar::tpcc::Context context;

  scar::tpcc::Database<scar::Silo> db;
  db.initialize(context, 4, 4);
  EXPECT_EQ(true, true);
}
