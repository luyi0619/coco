//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/tpcc/Database.h"
#include <gtest/gtest.h>

TEST(TestTPCCDatabase, TestBasic) {

  coco::tpcc::Context context;
  context.coordinator_num = 1;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  coco::tpcc::Database db;
  db.initialize(context);

  auto warehouse =
      sizeof(coco::tpcc::warehouse::key) + sizeof(coco::tpcc::warehouse::value);
  auto district =
      sizeof(coco::tpcc::district::key) + sizeof(coco::tpcc::district::value);
  auto customer =
      sizeof(coco::tpcc::customer::key) + sizeof(coco::tpcc::customer::value);
  auto customer_name_idx = sizeof(coco::tpcc::customer_name_idx::key) +
                           sizeof(coco::tpcc::customer_name_idx::value);
  auto history =
      sizeof(coco::tpcc::history::key) + sizeof(coco::tpcc::history::value);
  auto new_order =
      sizeof(coco::tpcc::new_order::key) + sizeof(coco::tpcc::new_order::value);
  auto order =
      sizeof(coco::tpcc::order::key) + sizeof(coco::tpcc::order::value);
  auto order_line = sizeof(coco::tpcc::order_line::key) +
                    sizeof(coco::tpcc::order_line::value);
  auto item = sizeof(coco::tpcc::item::key) + sizeof(coco::tpcc::item::value);
  auto stock =
      sizeof(coco::tpcc::stock::key) + sizeof(coco::tpcc::stock::value);

  auto total_size = warehouse + 10 * district + 10 * 3000 * customer +
                    10 * 3000 * customer_name_idx + 10 * 3000 * history +
                    10 * 3000 * new_order + 10 * 3000 * order +
                    10 * 3000 * 10 * order_line + 100000 * stock;

  LOG(INFO) << "TPC-C total size: " << total_size << " bytes.";

  EXPECT_EQ(true, true);
}