//
// Created by Yi Lu on 7/24/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Workload.h"
#include "core/Worker.h"
#include "protocol/Silo.h"
#include <gtest/gtest.h>

TEST(TestWorker, TestTPCC) {

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<ProtocolType>;

  scar::tpcc::Context context;
  context.partitionNum = 4;
  context.workerNum = 4;
  scar::tpcc::Random random;

  scar::tpcc::Database<MetaDataType> db;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;
  scar::Worker<WorkloadType> w(0, db, context, epoch, stopFlag);

  EXPECT_EQ(true, true);
}

TEST(TestWorker, TestYCSB) {

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::ycsb::Database<MetaDataType>>;
  using WorkloadType = scar::ycsb::Workload<ProtocolType>;

  scar::ycsb::Context context;
  context.partitionNum = 4;
  context.workerNum = 4;
  scar::ycsb::Random random;

  scar::ycsb::Database<MetaDataType> db;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;
  scar::Worker<WorkloadType> w(0, db, context, epoch, stopFlag);

  EXPECT_EQ(true, true);
}