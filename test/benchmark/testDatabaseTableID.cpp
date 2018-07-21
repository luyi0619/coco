//
// Created by Yi Lu on 7/21/18.
//

#include <gtest/gtest.h>
#include "benchmark/tpcc/Schema.h"
#include "benchmark/ycsb/Schema.h"

TEST(TestDatabaseTableID, TestBasic) {
    scar::tpcc::warehouse w;
    EXPECT_EQ(scar::tpcc::warehouse::tableID(), 0);
    EXPECT_EQ(scar::tpcc::district::tableID(), 1);
    EXPECT_EQ(scar::tpcc::customer::tableID(), 2);
    EXPECT_EQ(scar::tpcc::customer_name_idx::tableID(), 3);
    EXPECT_EQ(scar::tpcc::history::tableID(), 4);
    EXPECT_EQ(scar::tpcc::new_order::tableID(), 5);
    EXPECT_EQ(scar::tpcc::order::tableID(), 6);
    EXPECT_EQ(scar::tpcc::order_line::tableID(), 7);
    EXPECT_EQ(scar::tpcc::item::tableID(), 8);
    EXPECT_EQ(scar::tpcc::stock::tableID(), 9);

    EXPECT_EQ(scar::ycsb::ycsb::tableID(), 0);

}
