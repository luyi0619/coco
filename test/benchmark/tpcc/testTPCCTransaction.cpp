//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Transaction.h"
#include "protocol/Silo.h"
#include <gtest/gtest.h>

TEST(TestTPCCTransaction, TestBasic) {

  scar::tpcc::Database<scar::Silo> db;
  scar::tpcc::Context context;
  scar::tpcc::Random random;

  std::atomic<uint64_t> epoch;
  scar::Silo silo(epoch);
  scar::tpcc::NewOrder<scar::Silo> t1(db, context, random, silo);
  scar::tpcc::Payment<scar::Silo> t2(db, context, random, silo);
  EXPECT_EQ(true, true);
}
