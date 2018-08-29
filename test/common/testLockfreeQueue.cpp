//
// Created by Yi Lu on 8/29/18.
//

#include "common/LockfreeQueue.h"
#include <gtest/gtest.h>

TEST(TestLockfreeQueue, TestInt) {
  scar::LockfreeQueue<int> q;
  q.push(1);
  EXPECT_EQ(q.front(), 1);
  q.pop();

  EXPECT_EQ(q.write_available(), q.capacity());
}