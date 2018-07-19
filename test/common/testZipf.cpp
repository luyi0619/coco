//
// Created by Yi Lu on 7/19/18.
//
#include <vector>
#include <numeric>
#include <gtest/gtest.h>
#include "common/Zipf.h"
#include "common/Random.h"

TEST(TestZipf, TestBasic) {

    constexpr int N = 1000000, M = 1000000;
    auto &z = scar::Zipf::globalZipf();
    z.init(N, 0.9);
    scar::Random random;
    std::vector<int> v(N, 0);
    std::vector<int> percentile(10, 0);
    for (auto i = 0; i < M; i++) {
        double d = random.next_double();
        int k = z.value(d);
        v[k]++;
        percentile[k / (N / 10)]++;
    }

//    for(int i = 0;i < 10; i ++)
//    {
//        DLOG(INFO) << i << " " << 1.0 * std::accumulate(percentile.begin(), percentile.begin() + i + 1, 0) / M;
//    }

    EXPECT_GE(1.0 * percentile[0] / M, 0.7);
    EXPECT_LE(1.0 * percentile[0] / M, 0.75);
}