//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Transaction.h"
#include "protocol/Silo.h"
#include <gtest/gtest.h>

TEST(TestTPCCTransaction, TestBasic) {

  using DataT = std::atomic<uint64_t>;

  scar::tpcc::Database<DataT> db;
  scar::tpcc::Context context;
  scar::tpcc::Random random;

  std::atomic<uint64_t> epoch;
  scar::Silo<decltype(db)> silo(db, epoch);
  scar::tpcc::NewOrder<decltype(silo)> t1(db, context, random, silo);
  scar::tpcc::Payment<decltype(silo)> t2(db, context, random, silo);
  EXPECT_EQ(true, true);
}
