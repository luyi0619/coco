//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Transaction.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

TEST(TestTPCCTransaction, TestBasic) {

  using DatabaseType = coco::tpcc::Database;

  DatabaseType db;
  coco::tpcc::Context context;
  coco::tpcc::Random random;

  coco::HashPartitioner partitioner(0, 1);

  coco::tpcc::Storage storage;

  coco::Silo<decltype(db)> silo(db, context, partitioner);
  coco::tpcc::NewOrder<coco::SiloTransaction> t1(0, 0, db, context, random,
                                                 partitioner, storage);
  coco::tpcc::Payment<coco::SiloTransaction> t2(0, 0, db, context, random,
                                                partitioner, storage);
  EXPECT_EQ(true, true);
}
