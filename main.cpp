#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  using MetaDataType = std::atomic<uint64_t>;
  using TransactionType = scar::Transaction<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<TransactionType>;

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  int n = FLAGS_threads;
  scar::tpcc::Context context;
  context.protocol = "RStore";
  context.coordinatorNum = peers.size();
  context.partitionNum = n * context.coordinatorNum;
  context.workerNum = n;

  scar::tpcc::Database<MetaDataType> db;
  db.initialize(context, context.partitionNum, n);

  scar::Coordinator<WorkloadType> c(FLAGS_id, peers, db, context);
  c.connectToPeers();
  c.start();
  return 0;
}