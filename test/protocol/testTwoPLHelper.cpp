//
// Created by Yi Lu on 9/11/18.
//

#include "protocol/TwoPL/TwoPLHelper.h"
#include <atomic>
#include <gtest/gtest.h>

TEST(TestTwoPLHelper, TestBasic) {

  using scar::TwoPLHelper;
  std::atomic<uint64_t> a;

  EXPECT_EQ(TwoPLHelper::read_lock_max(), 511);
  EXPECT_FALSE(TwoPLHelper::is_read_locked(0));
  EXPECT_FALSE(TwoPLHelper::is_write_locked(0));
  EXPECT_EQ(TwoPLHelper::read_lock_num(0), 0);

  for(int i = 0; i < 511; i ++){
    EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), i);
    EXPECT_TRUE(TwoPLHelper::read_lock(a));
  }
  EXPECT_FALSE(TwoPLHelper::read_lock(a));
  EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), 511);

  for(int i = 0; i < 511; i ++){
    EXPECT_EQ(TwoPLHelper::read_lock_num(a), 511 - i);
    TwoPLHelper::read_lock_release(a);
  }
  EXPECT_EQ(TwoPLHelper::read_lock_num(a), 0);

  EXPECT_TRUE(TwoPLHelper::write_lock(a));
  EXPECT_FALSE(TwoPLHelper::read_lock(a));
  TwoPLHelper::write_lock_release(a);

  EXPECT_TRUE(TwoPLHelper::read_lock(a));
  EXPECT_FALSE(TwoPLHelper::write_lock(a));
  TwoPLHelper::read_lock_release(a);
}


TEST(TestTwoPLHelper, TestValue) {

  using scar::TwoPLHelper;
  std::atomic<uint64_t> a;
  a.store(0x1234);

  EXPECT_EQ(TwoPLHelper::read_lock_max(), 511);
  EXPECT_FALSE(TwoPLHelper::is_read_locked(0));
  EXPECT_FALSE(TwoPLHelper::is_write_locked(0));
  EXPECT_EQ(TwoPLHelper::read_lock_num(0), 0);

  for(int i = 0; i < 511; i ++){
    EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), i);
    EXPECT_TRUE(TwoPLHelper::read_lock(a));
  }
  EXPECT_FALSE(TwoPLHelper::read_lock(a));
  EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), 511);

  for(int i = 0; i < 511; i ++){
    EXPECT_EQ(TwoPLHelper::read_lock_num(a), 511 - i);
    TwoPLHelper::read_lock_release(a);
  }
  EXPECT_EQ(TwoPLHelper::read_lock_num(a), 0);

  EXPECT_TRUE(TwoPLHelper::write_lock(a));
  EXPECT_FALSE(TwoPLHelper::read_lock(a));
  TwoPLHelper::write_lock_release(a);

  EXPECT_TRUE(TwoPLHelper::read_lock(a));
  EXPECT_FALSE(TwoPLHelper::write_lock(a));
  TwoPLHelper::read_lock_release(a);
}
