//
// Created by Yi Lu on 8/28/18.
//

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Silo/SiloTransaction.h"
#include <gtest/gtest.h>

DEFINE_int32(threads, 1, "the number of threads.");
DEFINE_string(servers, "127.0.0.1:10010;127.0.0.1:10011;127.0.0.1:10012",
              "semicolon-separated list of servers");

TEST(TestCoordinator, TestTPCC) {

  using MetaDataType = std::atomic<uint64_t>;
  using DatabaseType = scar::tpcc::Database<MetaDataType>;

  int n = FLAGS_threads;

  scar::tpcc::Context context;
  context.partition_num = n;
  context.worker_num = n;

  DatabaseType db;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;

  scar::Coordinator c(0, {"127.0.0.1:10010"}, db, context);

  EXPECT_EQ(true, true);
}

TEST(TestCoordinator, TestConnect) {

  using MetaDataType = std::atomic<uint64_t>;
  using DatabaseType = scar::tpcc::Database<MetaDataType>;

  int n = FLAGS_threads;

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  auto startCoordinator = [n, &peers](int id) {
    scar::tpcc::Context context;
    context.partition_num = n;
    context.worker_num = n;

    DatabaseType db;
    scar::Coordinator c(id, peers, db, context);
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
