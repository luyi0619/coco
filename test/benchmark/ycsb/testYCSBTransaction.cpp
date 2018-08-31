//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Transaction.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

TEST(TestYCSBTransaction, TestBasic) {

  using DataT = std::atomic<uint64_t>;

  scar::ycsb::Database<DataT> db;
  scar::ycsb::Context context;
  scar::ycsb::Random random;

  std::atomic<uint64_t> epoch;
  scar::Silo<decltype(db)> silo(db, epoch);

  scar::ycsb::ReadModifyWrite<decltype(silo)> t(db, context, random, silo);
  EXPECT_EQ(true, true);
}
