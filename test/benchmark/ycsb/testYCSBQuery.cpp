//
// Created by Yi Lu on 7/19/18.
//

#include <gtest/gtest.h>
#include "benchmark/ycsb/Query.h"

TEST(TestYCSBQuery, TestBasic) {

    scar::ycsb::Context context;
    context.strategy = scar::ycsb::PartitionStrategy::ROUND_ROBIN;
    context.keysPerPartition = 20;
    context.keysPerTransaction = 10;
    context.crossPartitionProbability = 50;
    context.readWriteRatio = 80;
    context.readOnlyTransaction = 80;
    context.isUniform = true;

    context.partitionNum = 10;
    context.workerNum = 10;

    scar::ycsb::Random random(reinterpret_cast<uint64_t >(&context));

    constexpr int N = 10000, M = 10;
    constexpr int partitionID = 0;

    int readOnly = 0, reads = 0, writes = 0;

    for (auto i = 0; i < N; i++) {
        scar::ycsb::YCSBQuery<M> q = scar::ycsb::makeYCSBQuery<M>()(context, partitionID, random);

        bool hasWrite = false;
        int read = 0, write = 0;
        // test partitionID
        for (int k = 0; k < M; k++) {
            if (q.UPDATE[k]) {
                hasWrite = true;
            }
            read += !q.UPDATE[k];
            write += q.UPDATE[k];
        }
        if (hasWrite) {
            reads += read;
            writes += write;
        } else {
            readOnly++;
        }
        EXPECT_EQ(read + write, M);
    }
    EXPECT_GE(1.0 * readOnly / N, 0.8);
    EXPECT_LE(1.0 * reads / (reads + writes), 0.8);
}