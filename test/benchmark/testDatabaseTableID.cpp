//
// Created by Yi Lu on 7/21/18.
//

#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"
#include <gtest/gtest.h>

TEST(TestDatabaseTableID, TestBasic) {
  coco::tpcc::warehouse w;
  auto warehouseTableID = coco::tpcc::warehouse::tableID;
  EXPECT_EQ(warehouseTableID, 0);
  auto districtTableID = coco::tpcc::district::tableID;
  EXPECT_EQ(districtTableID, 1);
  auto customerTableID = coco::tpcc::customer::tableID;
  EXPECT_EQ(customerTableID, 2);
  auto customerNameIdxTableID = coco::tpcc::customer_name_idx::tableID;
  EXPECT_EQ(customerNameIdxTableID, 3);
  auto historyTableID = coco::tpcc::history::tableID;
  EXPECT_EQ(historyTableID, 4);
  auto newOrderTableID = coco::tpcc::new_order::tableID;
  EXPECT_EQ(newOrderTableID, 5);
  auto orderTableID = coco::tpcc::order::tableID;
  EXPECT_EQ(orderTableID, 6);
  auto orderLineTableID = coco::tpcc::order_line::tableID;
  EXPECT_EQ(orderLineTableID, 7);
  auto itemTableID = coco::tpcc::item::tableID;
  EXPECT_EQ(itemTableID, 8);
  auto stockTableID = coco::tpcc::stock::tableID;
  EXPECT_EQ(stockTableID, 9);

  auto ycsbTableID = coco::ycsb::ycsb::tableID;
  EXPECT_EQ(ycsbTableID, 0);
}
