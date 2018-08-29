//
// Created by Yi Lu on 8/29/18.
//

#include "jemalloc/jemalloc.h"
#include <gtest/gtest.h>

TEST(TestJemalloc, TestBasic) {

  const char *j;
  size_t s = sizeof(j);
  mallctl("version", &j, &s, NULL, 0);
  std::cout << j << std::endl;

  EXPECT_EQ(true, true);
}