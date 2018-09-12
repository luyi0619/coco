//
// Created by Yi Lu on 7/22/18.
//

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"
#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloTransaction.h"
#include <gtest/gtest.h>

TEST(TestYCSBTransaction, TestBasic) {

  using MetaDataType = std::atomic<uint64_t>;
  using DatabaseType = scar::ycsb::Database<MetaDataType>;

  DatabaseType db;
  scar::ycsb::Context context;
  scar::ycsb::Random random;

  scar::HashPartitioner partitioner(0, 1);

  scar::Silo<decltype(db)> silo(db, partitioner);

  scar::ycsb::Storage storage;

  scar::ycsb::ReadModifyWrite<scar::SiloTransaction> t(
      0, 0, 0, db, context, random, partitioner, storage);
  EXPECT_EQ(true, true);
}
