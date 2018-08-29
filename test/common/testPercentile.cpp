//
// Created by Yi Lu on 8/29/18.
//

#include "common/Percentile.h"
#include <gtest/gtest.h>

TEST(TestPercentile, TestBasic) {

  scar::Percentile<int> p;
  std::vector<int> data = {15, 20, 35, 40, 50};

  std::random_shuffle(data.begin(), data.end());

  p.add(data);

  EXPECT_EQ(p.nth(5), 15);
  EXPECT_EQ(p.nth(30), 20);
  EXPECT_EQ(p.nth(40), 20);
  EXPECT_EQ(p.nth(50), 35);
  EXPECT_EQ(p.nth(100), 50);

  data = {3, 6, 7, 8, 8, 10, 13, 15, 16, 20};
  std::random_shuffle(data.begin(), data.end());
  p.clear();
  p.add(data);

  EXPECT_EQ(p.nth(25), 7);
  EXPECT_EQ(p.nth(50), 8);
  EXPECT_EQ(p.nth(75), 15);
  EXPECT_EQ(p.nth(100), 20);

  data = {3, 6, 7, 8, 8, 9, 10, 13, 15, 16, 20};
  std::random_shuffle(data.begin(), data.end());
  p.clear();
  p.add(data);

  EXPECT_EQ(p.nth(25), 7);
  EXPECT_EQ(p.nth(50), 9);
  EXPECT_EQ(p.nth(75), 15);
  EXPECT_EQ(p.nth(100), 20);
}

TEST(TestPercentile, TestString) {

  scar::Percentile<std::string> p;
  std::vector<std::string> data = {"15", "20", "35", "40", "50"};

  std::random_shuffle(data.begin(), data.end());

  p.add(data);

  EXPECT_EQ(p.nth(5), "15");
  EXPECT_EQ(p.nth(30), "20");
  EXPECT_EQ(p.nth(40), "20");
  EXPECT_EQ(p.nth(50), "35");
  EXPECT_EQ(p.nth(100), "50");

  data = {"03", "06", "07", "08", "08", "10", "13", "15", "16", "20"};
  std::random_shuffle(data.begin(), data.end());
  p.clear();
  p.add(data);

  EXPECT_EQ(p.nth(25), "07");
  EXPECT_EQ(p.nth(50), "08");
  EXPECT_EQ(p.nth(75), "15");
  EXPECT_EQ(p.nth(100), "20");

  data = {"03", "06", "07", "08", "08", "09", "10", "13", "15", "16", "20"};
  std::random_shuffle(data.begin(), data.end());
  p.clear();
  p.add(data);

  EXPECT_EQ(p.nth(25), "07");
  EXPECT_EQ(p.nth(50), "09");
  EXPECT_EQ(p.nth(75), "15");
  EXPECT_EQ(p.nth(100), "20");
}