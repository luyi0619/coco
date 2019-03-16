//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Transaction.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

TEST(TestTPCCTransaction, TestBasic) {

  using DatabaseType = scar::tpcc::Database;

  DatabaseType db;
  scar::tpcc::Context context;
  scar::tpcc::Random random;

  scar::HashPartitioner partitioner(0, 1);

  scar::tpcc::Storage storage;

  scar::Silo<decltype(db)> silo(db, context, partitioner);
  scar::tpcc::NewOrder<scar::SiloTransaction> t1(0, 0, db, context, random,
                                                 partitioner, storage);
  scar::tpcc::Payment<scar::SiloTransaction> t2(0, 0, db, context, random,
                                                partitioner, storage);
  EXPECT_EQ(true, true);
}
