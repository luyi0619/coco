//
// Created by Yi Lu on 7/22/18.
//

#include <thread>

#include "common/Time.h"
#include <gtest/gtest.h>

TEST(TestZipf, TestBasic) {
  auto now1 = coco::Time::now();
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  auto now2 = coco::Time::now();
  EXPECT_LE(now2 - now1, 1000000 * 1.5);
}