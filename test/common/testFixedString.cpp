//
// Created by Yi Lu on 7/13/18.
//

#include "common/FixedString.h"
#include <gtest/gtest.h>

TEST(TestCommonFixedString, TestHashCode) {
  using namespace coco;
  FixedString<10> s1 = "123";
  EXPECT_EQ(ClassOf<decltype(s1)>::size(), 10);

  FixedString<10> s2;
  s2.assign("123");
  EXPECT_EQ(s1.hash_code(), s2.hash_code());

  FixedString<10> s3 = "123" + std::string(7, ' ');
  EXPECT_EQ(s1, s3);
  EXPECT_EQ(s1.hash_code(), s3.hash_code());
}
