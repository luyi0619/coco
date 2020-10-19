//
// Created by Yi Lu on 7/14/18.
//

#include "common/HashMap.h"
#include <gtest/gtest.h>
#include <thread>

TEST(TestHashMap, TestConcurrent) {

  constexpr int nThreads = 10, keys = 1000, totalKeys = nThreads * keys;

  coco::HashMap<nThreads, int, int> maps;
  std::vector<std::thread> v;

  for (int i = 0; i < nThreads; i++) {
    v.emplace_back(std::thread([i, &maps]() {
      for (int j = i; j < totalKeys; j += nThreads) {
        maps[j] = j;
      }
    }));
  }
  for (auto &t : v) {
    t.join();
  }

  EXPECT_EQ(maps.size(), totalKeys);
}

TEST(TestHashMap, TestSerialized) {
  coco::HashMap<1, int, int> maps;
  EXPECT_EQ(maps.size(), 0);
  maps[0] = 0;
  EXPECT_EQ(maps.size(), 1);
  auto &v = maps[0];
  EXPECT_EQ(v, 0);
  v = 1;
  EXPECT_EQ(maps[0], 1);
  EXPECT_FALSE(maps.contains(1));
  EXPECT_TRUE(maps.contains(0));
  EXPECT_FALSE(maps.insert(0, 2));
  maps.remove(0);
  EXPECT_EQ(maps.size(), 0);
  EXPECT_TRUE(maps.insert(0, 2));
  EXPECT_EQ(maps[0], 2);
}