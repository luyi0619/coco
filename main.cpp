#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Silo/Silo.h"
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_int32(threads, 1, "the number of threads.");
DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");

// ./main --logtostderr=1 -threads=2

int main(int argc, char *argv[]) {
  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  using MetaDataType = std::atomic<uint64_t>;
  using ProtocolType = scar::Silo<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<ProtocolType>;

  int n = FLAGS_threads;
  scar::tpcc::Context context;
  context.partitionNum = n;
  context.workerNum = n;

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  scar::tpcc::Database<MetaDataType> db;
  db.initialize(context, n, n);

  scar::Coordinator<WorkloadType> c(0, peers, db, context);
  c.start();
  return 0;
}