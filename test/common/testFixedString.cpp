//
// Created by Yi Lu on 7/13/18.
//

#include "common/FixedString.h"
#include <gtest/gtest.h>

TEST(TestCommonFixedString, TestHashCode) {
  scar::FixedString<10> s1 = "123";
  scar::FixedString<10> s2;
  s2.assign("123");
  EXPECT_EQ(s1.hash_code(), s2.hash_code());
}
