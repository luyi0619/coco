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

  scar::tpcc::Context context;
  context.partitionNum = 4;
  context.workerNum = 4;
  scar::tpcc::Random random;

  scar::tpcc::Database<scar::Silo> db;
  db.initialize(context, 4, 4);

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;
  scar::Silo protocol(epoch);

  scar::tpcc::Workload<decltype(db)> workload(db, context, random, protocol);

  scar::Worker<decltype(workload)> w(db, context, epoch, stopFlag);
  w.start();

  EXPECT_EQ(true, true);
}

TEST(TestWorker, TestYCSB) {

  scar::ycsb::Context context;
  context.partitionNum = 4;
  context.workerNum = 4;
  scar::ycsb::Random random;

  scar::ycsb::Database<scar::Silo> db;
  db.initialize(context, 4, 4);

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;
  scar::Silo protocol(epoch);

  scar::ycsb::Workload<decltype(db)> workload(db, context, random, protocol);

  scar::Worker<decltype(workload)> w(db, context, epoch, stopFlag);
  w.start();

  EXPECT_EQ(true, true);
}