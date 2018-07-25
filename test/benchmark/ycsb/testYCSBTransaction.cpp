//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Transaction.h"
#include "protocol/Silo.h"
#include <gtest/gtest.h>

TEST(TestYCSBTransaction, TestBasic) {

  scar::ycsb::Database<scar::Silo> db;
  scar::ycsb::Context context;
  scar::ycsb::Random random;

  std::atomic<uint64_t> epoch;
  scar::Silo silo(epoch);

  scar::ycsb::ReadModifyWrite<scar::ycsb::Database<scar::Silo>> t(db, context,
                                                                  random, silo);
  EXPECT_EQ(true, true);
}
