//
// Created by Yi Lu on 9/19/18.
//

#include "protocol/Scar/ScarHelper.h"
#include <gtest/gtest.h>

TEST(TestTwoPLHelper, TestBasic) {

  using scar::ScarHelper;

  uint64_t wts = 0x1234;
  uint64_t delta = 0x2345;
  uint64_t v = 0;
  uint64_t v1 = ScarHelper::set_wts(v, wts);
  EXPECT_EQ(v1, wts);
  uint64_t v2 = ScarHelper::set_delta(v1, delta);
  EXPECT_EQ(delta, ScarHelper::get_delta(v2));
  uint64_t rts = ScarHelper::get_rts(v2);
  EXPECT_EQ(rts, wts + delta);
  uint64_t new_rts = wts + 2 * delta;
  uint64_t v3 = ScarHelper::set_rts(v2, new_rts);
  EXPECT_EQ(wts, ScarHelper::get_wts(v3));
  EXPECT_EQ(delta * 2, ScarHelper::get_delta(v3));
  EXPECT_EQ(new_rts, ScarHelper::get_rts(v3));
}

TEST(TestTwoPLHelper, TestOverflow) {

  using scar::ScarHelper;

  uint64_t wts = 0x1234;

  uint64_t v = 0;
  uint64_t v1 = ScarHelper::set_wts(v, wts);
  uint64_t rts = wts + (1ull << 15) - 1;
  uint64_t v2 = ScarHelper::set_rts(v1, rts);
  EXPECT_EQ(ScarHelper::get_wts(v2), wts);
  EXPECT_EQ(ScarHelper::get_delta(v2), 0x7fffull);
  EXPECT_EQ(ScarHelper::get_rts(v2), rts);

  uint64_t rts2 = wts + (1ull << 15);
  uint64_t v3 = ScarHelper::set_rts(v1, rts2);
  EXPECT_EQ(ScarHelper::get_rts(v3), rts2);
  EXPECT_EQ(ScarHelper::get_rts(v3),
            ScarHelper::get_wts(v3) + ScarHelper::get_delta(v3));

  uint64_t rts3 = wts + 0x12345678;
  uint64_t v4 = ScarHelper::set_rts(v1, rts3);
  EXPECT_EQ(ScarHelper::get_rts(v4), rts3);
  EXPECT_EQ(ScarHelper::get_rts(v4),
            ScarHelper::get_wts(v4) + ScarHelper::get_delta(v4));
}