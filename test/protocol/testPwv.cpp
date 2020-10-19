//
// Created by Yi Lu on 1/14/20.
//

#include "benchmark/tpcc/Query.h"
#include "benchmark/ycsb/Query.h"
#include "protocol/Pwv/PwvStatement.h"

#include <gtest/gtest.h>

TEST(TestPwvYCSBStatement, TestBasic) {

  using namespace coco;

  coco::ycsb::Context context;
  context.strategy = coco::ycsb::PartitionStrategy::ROUND_ROBIN;
  context.keysPerPartition = 20;
  context.keysPerTransaction = 10;
  context.partition_num = 4;
  context.worker_num = 4;
  context.coordinator_num = 1;
  context.partitioner = "hash";
  coco::ycsb::Database db;
  db.initialize(context);
  coco::ycsb::Storage storage;
  ycsb::Random random(reinterpret_cast<uint64_t>(&context));
  constexpr int partitionID = 0;

  coco::ycsb::YCSBQuery<10> q =
      coco::ycsb::makeYCSBQuery<10>()(context, partitionID, random);

  auto statement = std::make_unique<PwvYCSBStatement>(
      db, context, random, storage, partitionID, q, 0);
}

TEST(TestPwvNewOrderStatement, TestBasic) {

  using namespace coco;

  coco::tpcc::Context context;
  context.partition_num = 4;
  context.worker_num = 4;
  context.coordinator_num = 1;
  context.partitioner = "hash";
  coco::tpcc::Database db;
  db.initialize(context);
  coco::tpcc::Storage storage;
  tpcc::Random random(reinterpret_cast<uint64_t>(&context));
  constexpr int partitionID = 0;
  coco::tpcc::NewOrderQuery q1 =
      coco::tpcc::makeNewOrderQuery()(context, 1, random);
  float total_amount;
  std::atomic<int> rvp;
  PwvNewOrderWarehouseStatement s1(db, context, random, storage, partitionID,
                                   q1);
  PwvNewOrderStockStatement s2(db, context, random, storage, partitionID, q1, 0,
                               rvp);
  PwvNewOrderOrderStatement s3(db, context, random, storage, partitionID, q1,
                               total_amount);

  coco::tpcc::PaymentQuery q2 =
      coco::tpcc::makePaymentQuery()(context, 1, random);

  PwvPaymentDistrictStatement s4(db, context, random, storage, partitionID, q2);
  PwvPaymentDistrictStatement s5(db, context, random, storage, partitionID, q2);
}