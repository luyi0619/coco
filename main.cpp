#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Coordinator.h"
#include "protocol/Silo.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_int32(threads, 1, "the number of threads.");

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

  scar::tpcc::Database<MetaDataType> db;
  db.initialize(context, n, n);

  scar::Coordinator<WorkloadType> c(0, db, context);
  c.start();
  return 0;
}