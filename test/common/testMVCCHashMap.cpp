//
// Created by Yi Lu on 2019-09-02.
//

#include "common/MVCCHashMap.h"
#include <gtest/gtest.h>
#include <thread>

TEST(TestHashMap, TestBasic) {
  coco::MVCCHashMap<10, int, int> map;
  map.insert_key_version_holder(1, 100) = 10;

  EXPECT_TRUE(map.contains_key(1));
  EXPECT_TRUE(map.contains_key_version(1, 100));
  EXPECT_FALSE(map.contains_key_version(1, 200));

  map.insert_key_version_holder(1, 200) = 10;
  map.insert_key_version_holder(1, 300) = 10;

  EXPECT_EQ(map.version_count(1), 3);

  map.remove_key_version(1, 200);
  map.remove_key_version(1, 300);

  EXPECT_EQ(map.version_count(1), 1);

  map.remove_key(1);
  EXPECT_EQ(map.version_count(1), 0);

  map.insert_key_version_holder(1, 400) = 10;
  map.insert_key_version_holder(1, 500) = 10;
  map.insert_key_version_holder(1, 600) = 10;

  EXPECT_EQ(map.version_count(1), 3);
  map.vacuum_key_versions(1, 500);

  EXPECT_EQ(map.version_count(1), 1);

  map.insert_key_version_holder(1, 700) = 20;
  map.insert_key_version_holder(1, 800) = 30;
  map.insert_key_version_holder(1, 900) = 40;
  map.insert_key_version_holder(1, 1000) = 50;

  EXPECT_EQ(*map.get_key_version(1, 700), 20);
  EXPECT_EQ(*map.get_key_version(1, 800), 30);
  EXPECT_EQ(map.get_key_version(1, 1200), nullptr);

  EXPECT_EQ(*map.get_key_version_prev(1, 1200), 50);
  EXPECT_EQ(*map.get_key_version_prev(1, 601), 10);
  EXPECT_EQ(map.get_key_version_prev(1, 600), nullptr);

  map.vacuum_key_keep_latest(1);

  EXPECT_EQ(map.version_count(1), 1);
  EXPECT_FALSE(map.contains_key_version(1, 1200));
  EXPECT_FALSE(map.contains_key_version(1, 900));
  EXPECT_TRUE(map.contains_key_version(1, 1000));
}
