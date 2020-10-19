//
// Created by Yi Lu on 7/14/18.
//

#include "benchmark/ycsb/Random.h"
#include <gtest/gtest.h>

TEST(TestTPCCRandom, TestRandStr) {
  coco::ycsb::Random random;
  std::string rand_str = random.rand_str(100);
  EXPECT_EQ(rand_str.length(), 100);
}
