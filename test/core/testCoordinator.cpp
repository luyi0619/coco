//
// Created by Yi Lu on 8/28/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Silo/Silo.h"
#include <gtest/gtest.h>

DEFINE_int32(threads, 1, "the number of threads.");
DEFINE_string(servers, "127.0.0.1:10010;127.0.0.1:10011;127.0.0.1:10012",
              "semicolon-separated list of servers");

TEST(TestCoordinator, TestTPCC) {

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<ProtocolType>;

  int n = FLAGS_threads;

  scar::tpcc::Context context;
  context.partitionNum = n;
  context.workerNum = n;

  scar::tpcc::Database<MetaDataType> db;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;

  scar::Coordinator<WorkloadType> c(0, {"127.0.0.1"}, db, context);

  EXPECT_EQ(true, true);
}

TEST(TestCoordinator, TestConnect) {

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<ProtocolType>;

  int n = FLAGS_threads;

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  auto startCoordinator = [n, &peers](int id) {
    scar::tpcc::Context context;
    context.partitionNum = n;
    context.workerNum = n;

    scar::tpcc::Database<MetaDataType> db;
    scar::Coordinator<WorkloadType> c(id, peers, db, context);
    c.connectToPeers();
  };

  std::vector<std::thread> v;
  for (auto i = 0; i < 3; i++) {
    v.emplace_back(startCoordinator, i);
  }

  for (auto i = 0; i < 3; i++) {
    v[i].join();
  }

  EXPECT_EQ(true, true);
}
