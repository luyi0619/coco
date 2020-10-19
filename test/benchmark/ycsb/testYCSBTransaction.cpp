//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Transaction.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

TEST(TestYCSBTransaction, TestBasic) {

  using DatabaseType = coco::ycsb::Database;

  DatabaseType db;
  coco::ycsb::Context context;
  context.partition_num = 1;

  coco::ycsb::Random random;

  coco::HashPartitioner partitioner(0, 1);

  coco::Silo<decltype(db)> silo(db, context, partitioner);

  coco::ycsb::Storage storage;

  coco::ycsb::ReadModifyWrite<coco::SiloTransaction> t(
      0, 0, db, context, random, partitioner, storage);
  EXPECT_EQ(true, true);
}
