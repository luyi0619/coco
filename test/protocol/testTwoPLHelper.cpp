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

  bool success;

  for (int i = 0; i < 511; i++) {
    EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), i);
    TwoPLHelper::read_lock(a, success);
    EXPECT_TRUE(success);
  }
  TwoPLHelper::read_lock(a, success);
  EXPECT_FALSE(success);
  EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), 511);

  for (int i = 0; i < 511; i++) {
    EXPECT_EQ(TwoPLHelper::read_lock_num(a), 511 - i);
    TwoPLHelper::read_lock_release(a);
  }
  EXPECT_EQ(TwoPLHelper::read_lock_num(a), 0);

  TwoPLHelper::write_lock(a, success);
  EXPECT_TRUE(success);
  TwoPLHelper::read_lock(a, success);
  EXPECT_FALSE(success);
  TwoPLHelper::write_lock_release(a);

  TwoPLHelper::read_lock(a, success);
  EXPECT_TRUE(success);
  TwoPLHelper::write_lock(a, success);
  EXPECT_FALSE(success);
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

  bool success;

  for (int i = 0; i < 511; i++) {
    EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), i);
    TwoPLHelper::read_lock(a, success);
    EXPECT_TRUE(success);
  }
  TwoPLHelper::read_lock(a, success);
  EXPECT_FALSE(success);
  EXPECT_EQ(TwoPLHelper::read_lock_num(a.load()), 511);

  for (int i = 0; i < 511; i++) {
    EXPECT_EQ(TwoPLHelper::read_lock_num(a), 511 - i);
    TwoPLHelper::read_lock_release(a);
  }
  EXPECT_EQ(TwoPLHelper::read_lock_num(a), 0);

  TwoPLHelper::write_lock(a, success);
  EXPECT_TRUE(success);
  TwoPLHelper::read_lock(a, success);
  EXPECT_FALSE(success);
  TwoPLHelper::write_lock_release(a);

  TwoPLHelper::read_lock(a, success);
  EXPECT_TRUE(success);
  TwoPLHelper::write_lock(a, success);
  EXPECT_FALSE(success);
  TwoPLHelper::read_lock_release(a);
}
