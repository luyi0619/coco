//
// Created by Yi Lu on 7/15/18.
//

#include <gtest/gtest.h>
#include "benchmark/ycsb/Schema.h"

TEST(TestYCSBSchema, TestYCSB) {
    scar::ycsb::ycsb::key key(1);
    EXPECT_EQ(key.Y_KEY, 1);
    scar::ycsb::ycsb::value value;
    value.Y_F01 = "amazon";
    EXPECT_EQ(value.Y_F01, "amazon");
    scar::ycsb::ycsb::key key_ = scar::ycsb::ycsb::key(1);
    EXPECT_EQ(key, key_);
}
