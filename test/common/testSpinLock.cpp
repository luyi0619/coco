//
// Created by Yi Lu on 7/14/18.
//

#include "common/SpinLock.h"
#include <gtest/gtest.h>
#include <thread>

TEST(TestCommonSpinLock, TestLock) {
  coco::SpinLock lock;
  lock.lock();
  bool ok;
  std::thread t1([&]() {
    auto start = std::chrono::steady_clock::now();
    lock.lock();
    lock.unlock();
    auto stop = std::chrono::steady_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    auto elapsed = duration.count();
    ok = elapsed > 80 && elapsed < 120;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  lock.unlock();
  t1.join();
  EXPECT_EQ(ok, true);
}
