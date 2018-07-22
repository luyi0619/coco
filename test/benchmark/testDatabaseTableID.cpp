//
// Created by Yi Lu on 7/21/18.
//

#include <gtest/gtest.h>
#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"

TEST(TestDatabaseTableID, TestBasic) {
    scar::tpcc::warehouse w;
    auto warehouseTableID = scar::tpcc::warehouse::tableID;
    EXPECT_EQ(warehouseTableID, 0);
    auto districtTableID = scar::tpcc::district::tableID;
    EXPECT_EQ(districtTableID, 1);
    auto customerTableID = scar::tpcc::customer::tableID;
    EXPECT_EQ(customerTableID, 2);
    auto customerNameIdxTableID = scar::tpcc::customer_name_idx::tableID;
    EXPECT_EQ(customerNameIdxTableID, 3);
    auto historyTableID = scar::tpcc::history::tableID;
    EXPECT_EQ(historyTableID, 4);
    auto newOrderTableID = scar::tpcc::new_order::tableID;
    EXPECT_EQ(newOrderTableID, 5);
    auto orderTableID = scar::tpcc::order::tableID;
    EXPECT_EQ(orderTableID, 6);
    auto orderLineTableID = scar::tpcc::order_line::tableID;
    EXPECT_EQ(orderLineTableID, 7);
    auto itemTableID = scar::tpcc::item::tableID;
    EXPECT_EQ(itemTableID, 8);
    auto stockTableID = scar::tpcc::stock::tableID;
    EXPECT_EQ(stockTableID, 9);

    auto ycsbTableID = scar::ycsb::ycsb::tableID;
    EXPECT_EQ(ycsbTableID, 0);

}
