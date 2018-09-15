#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Calvin/CalvinTransaction.h"
#include "protocol/Silo/SiloTransaction.h"
#include "protocol/TwoPL/TwoPLTransaction.h"
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(lock_manager_num, 1, "the number of calvin lock manager");
DEFINE_int32(batch_size, 240, "rstore or calvin batch size");
DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");
DEFINE_string(protocol, "RStore", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  int n = FLAGS_threads;
  scar::tpcc::Context context;
  context.protocol = FLAGS_protocol;
  context.coordinator_num = peers.size();
  context.partition_num = n * context.coordinator_num;
  context.lock_manager_num = FLAGS_lock_manager_num;
  context.batch_size = FLAGS_batch_size;
  context.worker_num = n;
  context.replica_group = FLAGS_replica_group;

  // only create coordinator for tpc-c
  std::unordered_set<std::string> protocols = {"Silo",  "SiloGC",  "RStore",
                                               "TwoPL", "TwoPLGC", "Calvin"};
  std::unordered_set<std::string> silo_protocols = {"Silo", "SiloGC", "RStore"};
  std::unordered_set<std::string> twopl_protocols = {"TwoPL", "TwoPLGC"};

  CHECK(protocols.count(context.protocol) == 1);

  if (silo_protocols.count(context.protocol)) {
    // only create cooridnator for silo-like protocols
    using MetaDataType = std::atomic<uint64_t>;
    using TransactionType = scar::SiloTransaction;
    using WorkloadType = scar::tpcc::Workload<TransactionType>;

    scar::tpcc::Database<MetaDataType> db;

    db.initialize(context, context.partition_num, n);

    auto c = std::make_unique<scar::Coordinator<WorkloadType>>(FLAGS_id, peers,
                                                               db, context);

    c->connectToPeers();
    c->start();
  } else if (twopl_protocols.count(context.protocol)) {
    using MetaDataType = std::atomic<uint64_t>;
    using TransactionType = scar::TwoPLTransaction;
    using WorkloadType = scar::tpcc::Workload<TransactionType>;

    scar::tpcc::Database<MetaDataType> db;

    db.initialize(context, context.partition_num, n);

    auto c = std::make_unique<scar::Coordinator<WorkloadType>>(FLAGS_id, peers,
                                                               db, context);

    c->connectToPeers();
    c->start();
  } else if (context.protocol == "Calvin") {
    using MetaDataType = std::atomic<uint64_t>;
    using TransactionType = scar::CalvinTransaction;
    using WorkloadType = scar::tpcc::Workload<TransactionType>;

    scar::tpcc::Database<MetaDataType> db;

    db.initialize(context, context.partition_num, n);

    auto c = std::make_unique<scar::Coordinator<WorkloadType>>(FLAGS_id, peers,
                                                               db, context);

    c->connectToPeers();
    c->start();
  }

  return 0;
}