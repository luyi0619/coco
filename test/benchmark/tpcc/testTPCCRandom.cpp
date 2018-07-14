//
// Created by Yi Lu on 7/14/18.
//

#include <gtest/gtest.h>
#include "benchmark/tpcc/TPCCRandom.h"

TEST(TestTPCCRandom, TestRandStr) {
    scar::TPCCRandom random;
    std::string astring = random.a_string(10, 10);
    std::string nstring = random.a_string(10, 10);
    std::string zip = random.a_string(10, 10);
    std::string name = random.rand_last_name(123);
    EXPECT_EQ(astring.length(), 10);
    EXPECT_EQ(nstring.length(), 10);
    EXPECT_EQ(zip.length(), 10);
    EXPECT_GT(name.length(), 0);
}
