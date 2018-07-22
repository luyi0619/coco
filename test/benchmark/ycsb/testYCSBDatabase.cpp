//
// Created by Yi Lu on 7/21/18.
//

#include <gtest/gtest.h>
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Schema.h"
#include "protocol/Silo.h"

TEST(TestYCSBDatabase, TestBasic) {

    scar::ycsb::Context context;
    context.strategy = scar::ycsb::PartitionStrategy::ROUND_ROBIN;
    context.keysPerPartition = 20;
    context.keysPerTransaction = 10;

    scar::ycsb::Database<scar::Silo> db;
    db.initialize(context, 4, 4);
    EXPECT_EQ(true, true);
}
