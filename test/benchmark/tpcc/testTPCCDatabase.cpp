//
// Created by Yi Lu on 7/21/18.
//

#include <gtest/gtest.h>
#include "benchmark/tpcc/Database.h"
#include "protocol/Silo.h"

TEST(TestTPCCDatabase, TestBasic) {

    scar::tpcc::Database<scar::Silo> db;
    db.initialize(4, 4);
    EXPECT_EQ(true, true);
}
