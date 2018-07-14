//
// Created by Yi Lu on 7/14/18.
//

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <thread>
#include <chrono>
#include "common/Random.h"

TEST(TestRandom, TestRandomness) {

    constexpr int N = 100, nNumbers = 100000000;

    std::vector<int> v(N, 0);

    scar::Random r;

    for(int i = 0; i< nNumbers;i ++){
        int k = r.uniform_dist(0, N - 1);
        v[k] ++;
    }

    int min_e = *std::min_element(v.begin(), v.end());
    int max_e = *std::max_element(v.begin(), v.end());
    auto avg = 1.0 * nNumbers / N;

    EXPECT_LE( (max_e - min_e) / avg, 0.01);
}