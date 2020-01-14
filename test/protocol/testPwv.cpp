//
// Created by Yi Lu on 1/14/20.
//

#include "benchmark/tpcc/Query.h"
#include "benchmark/ycsb/Query.h"
#include "protocol/Pwv/PwvStatement.h"

#include <gtest/gtest.h>

TEST(TestPwvYCSBStatement, TestBasic) {

  using namespace scar;

  scar::ycsb::Context context;
  context.strategy = scar::ycsb::PartitionStrategy::ROUND_ROBIN;
  context.keysPerPartition = 20;
  context.keysPerTransaction = 10;
  context.partition_num = 4;
  context.worker_num = 4;
  context.coordinator_num = 1;
  context.partitioner = "hash";
  scar::ycsb::Database db;
  db.initialize(context);
  scar::ycsb::Storage storage;
  ycsb::Random random(reinterpret_cast<uint64_t>(&context));
  constexpr int partitionID = 0;

  scar::ycsb::YCSBQuery<10> q =
      scar::ycsb::makeYCSBQuery<10>()(context, partitionID, random);

  auto statement = std::make_unique<PwvYCSBStatement>(
      db, context, random, storage, partitionID, q, 0);
}

TEST(TestPwvNewOrderStatement, TestBasic) {

  using namespace scar;

  scar::tpcc::Context context;
  context.partition_num = 4;
  context.worker_num = 4;
  context.coordinator_num = 1;
  context.partitioner = "hash";
  scar::tpcc::Database db;
  db.initialize(context);
  scar::tpcc::Storage storage;
  tpcc::Random random(reinterpret_cast<uint64_t>(&context));
  constexpr int partitionID = 0;
  scar::tpcc::NewOrderQuery q1 =
      scar::tpcc::makeNewOrderQuery()(context, 1, random);

  PwvNewOrderWarehouseStatement s1(db, context, random, storage, partitionID,
                                   q1);
  PwvNewOrderStockStatement s2(db, context, random, storage, partitionID, q1,
                               0);
  PwvNewOrderOrderStatement s3(db, context, random, storage, partitionID, q1);

  scar::tpcc::PaymentQuery q2 =
      scar::tpcc::makePaymentQuery()(context, 1, random);

  PwvPaymentDistrictStatement s4(db, context, random, storage, partitionID, q2);
  PwvPaymentDistrictStatement s5(db, context, random, storage, partitionID, q2);
}