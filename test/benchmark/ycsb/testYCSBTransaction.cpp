//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Transaction.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

TEST(TestYCSBTransaction, TestBasic) {

  using DatabaseType = scar::ycsb::Database;

  DatabaseType db;
  scar::ycsb::Context context;
  context.partition_num = 1;

  scar::ycsb::Random random;

  scar::HashPartitioner partitioner(0, 1);

  scar::Silo<decltype(db)> silo(db, context, partitioner);

  scar::ycsb::Storage storage;

  scar::ycsb::ReadModifyWrite<scar::SiloTransaction> t(
      0, 0, db, context, random, partitioner, storage);
  EXPECT_EQ(true, true);
}
