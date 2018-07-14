//
// Created by Yi Lu on 7/14/18.
//

#include <gtest/gtest.h>
#include "benchmark/ycsb/YCSBRandom.h"

TEST(TestTPCCRandom, TestRandStr) {
    scar::YCSBRandom random;
    std::string rand_str = random.rand_str(100);
    EXPECT_EQ(rand_str.length(), 100);
}
