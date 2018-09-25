#include "benchmark/tpcc/Database.h"
#include "core/Coordinator.h"
#include <boost/algorithm/string.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(partition_num, 1, "the number of partitions");
DEFINE_int32(batch_size, 100, "rstore or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_int32(batch_flush, 10, "batch flush");
DEFINE_int32(sleep_time, 1000, "retry sleep time");
DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");
DEFINE_string(protocol, "RStore", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group");
DEFINE_string(partitioner, "hash", "database partitioner (hash, hash2, pb)");
DEFINE_bool(operation_replication, false, "use operation replication");
DEFINE_string(query, "neworder", "tpcc query, mixed, neworder, payment");
DEFINE_int32(neworder_dist, 10, "new order distributed.");
DEFINE_int32(payment_dist, 15, "payment distributed.");

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
  context.batch_size = FLAGS_batch_size;
  context.batch_flush = FLAGS_batch_flush;
  context.sleep_time = FLAGS_sleep_time;
  context.group_time = FLAGS_group_time;
  context.worker_num = n;
  context.partition_num = FLAGS_partition_num;
  context.replica_group = FLAGS_replica_group;
  context.operation_replication = FLAGS_operation_replication;
  context.partitioner = FLAGS_partitioner;

  if (FLAGS_query == "mixed") {
    context.workloadType = scar::tpcc::TPCCWorkloadType ::MIXED;
  } else if (FLAGS_query == "neworder") {
    context.workloadType = scar::tpcc::TPCCWorkloadType ::NEW_ORDER_ONLY;
  } else if (FLAGS_query == "payment") {
    context.workloadType = scar::tpcc::TPCCWorkloadType ::PAYMENT_ONLY;
  } else {
    CHECK(false);
  }

  context.newOrderCrossPartitionProbability = FLAGS_neworder_dist;
  context.paymentCrossPartitionProbability = FLAGS_payment_dist;

  using MetaDataType = std::atomic<uint64_t>;
  scar::tpcc::Database<MetaDataType> db;
  db.initialize(context, context.partition_num, n);

  auto c = std::make_unique<scar::Coordinator>(FLAGS_id, peers, db, context);
  c->connectToPeers();
  c->start();
  return 0;
}