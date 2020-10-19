//
// Created by Yi Lu on 7/15/18.
//

#include "benchmark/ycsb/Schema.h"
#include <gtest/gtest.h>

TEST(TestYCSBSchema, TestYCSB) {
  coco::ycsb::ycsb::key key(1);
  EXPECT_EQ(key.Y_KEY, 1);
  coco::ycsb::ycsb::value value;
  value.Y_F01 = "amazon";
  EXPECT_EQ(value.Y_F01, "amazon");
  coco::ycsb::ycsb::key key_ = coco::ycsb::ycsb::key(1);
  EXPECT_EQ(key, key_);
}
