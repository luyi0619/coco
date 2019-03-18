//
// Created by Yi Lu on 8/28/18.
//

#include "benchmark/tpcc/Database.h"
#include "core/Coordinator.h"
#include <gtest/gtest.h>

DEFINE_int32(threads, 1, "the number of threads.");
DEFINE_string(servers, "127.0.0.1:10010;127.0.0.1:10011;127.0.0.1:10012",
              "semicolon-separated list of servers");

TEST(TestCoordinator, TestTPCC) {

  using DatabaseType = scar::tpcc::Database;

  int n = FLAGS_threads;

  scar::tpcc::Context context;
  context.peers = {"127.0.0.1:10010"};
  context.coordinator_num = 1;
  context.partition_num = n;
  context.worker_num = n;
  context.protocol = "Silo";
  context.partitioner = "hash";

  DatabaseType db;

  std::atomic<uint64_t> epoch;
  std::atomic<bool> stopFlag;

  scar::Coordinator c(0, db, context);

  EXPECT_EQ(true, true);
}

TEST(TestCoordinator, TestConnect) {

  using DatabaseType = scar::tpcc::Database;

  int n = FLAGS_threads;

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  auto startCoordinator = [n, &peers](int id) {
    scar::tpcc::Context context;
    context.peers = peers;
    context.coordinator_num = 3;
    context.partition_num = n;
    context.worker_num = n;
    context.protocol = "Silo";
    context.partitioner = "hash";
    context.io_thread_num = 1;

    DatabaseType db;
    scar::Coordinator c(id, db, context);
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
