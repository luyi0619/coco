//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/tpcc/Database.h"
#include <gtest/gtest.h>

TEST(TestTPCCDatabase, TestBasic) {

  scar::tpcc::Context context;
  context.coordinator_num = 1;
  context.partition_num = 4;
  context.worker_num = 4;
  context.partitioner = "hash";
  scar::tpcc::Database db;
  db.initialize(context);

  auto warehouse =
      sizeof(scar::tpcc::warehouse::key) + sizeof(scar::tpcc::warehouse::value);
  auto district =
      sizeof(scar::tpcc::district::key) + sizeof(scar::tpcc::district::value);
  auto customer =
      sizeof(scar::tpcc::customer::key) + sizeof(scar::tpcc::customer::value);
  auto customer_name_idx = sizeof(scar::tpcc::customer_name_idx::key) +
                           sizeof(scar::tpcc::customer_name_idx::value);
  auto history =
      sizeof(scar::tpcc::history::key) + sizeof(scar::tpcc::history::value);
  auto new_order =
      sizeof(scar::tpcc::new_order::key) + sizeof(scar::tpcc::new_order::value);
  auto order =
      sizeof(scar::tpcc::order::key) + sizeof(scar::tpcc::order::value);
  auto order_line = sizeof(scar::tpcc::order_line::key) +
                    sizeof(scar::tpcc::order_line::value);
  auto item = sizeof(scar::tpcc::item::key) + sizeof(scar::tpcc::item::value);
  auto stock =
      sizeof(scar::tpcc::stock::key) + sizeof(scar::tpcc::stock::value);

  auto total_size = warehouse + 10 * district + 10 * 3000 * customer +
                    10 * 3000 * customer_name_idx + 10 * 3000 * history +
                    10 * 3000 * new_order + 10 * 3000 * order +
                    10 * 3000 * 10 * order_line + 100000 * stock;

  LOG(INFO) << "TPC-C total size: " << total_size << " bytes.";

  EXPECT_EQ(true, true);
}