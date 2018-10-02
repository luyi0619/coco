//
// Created by Yi Lu on 10/1/18.
//

#include "benchmark/retwis/Schema.h"
#include <gtest/gtest.h>

TEST(TestRETWISSchema, TestRETWIS) {
  scar::retwis::retwis::key key(1);
  EXPECT_EQ(key.KEY, 1);
  scar::retwis::retwis::value value;
  value.VALUE = "amazon";
  EXPECT_EQ(value.VALUE, "amazon");
  scar::retwis::retwis::key key_ = scar::retwis::retwis::key(1);
  EXPECT_EQ(key, key_);
}
