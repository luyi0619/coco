//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Transaction.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

TEST(TestTPCCTransaction, TestBasic) {

  using MetaDataType = std::atomic<uint64_t>;
  using DatabaseType = scar::tpcc::Database<MetaDataType>;
  using RWKeyType = scar::SiloRWKey;

  DatabaseType db;
  scar::tpcc::Context context;
  scar::tpcc::Random random;

  std::atomic<uint64_t> epoch;
  scar::Silo<decltype(db)> silo(db, epoch);
  scar::tpcc::NewOrder<RWKeyType, DatabaseType> t1(db, context, random);
  scar::tpcc::Payment<RWKeyType, DatabaseType> t2(db, context, random);
  EXPECT_EQ(true, true);
}
