//
// Created by Yi Lu on 10/1/18.
//

#include "benchmark/retwis/Database.h"
#include "core/Coordinator.h"

DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(partition_num, 1, "the number of partitions");
DEFINE_int32(batch_size, 100, "rstore or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_int32(batch_flush, 20, "batch flush");
DEFINE_int32(sleep_time, 1000, "retry sleep time");
DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");
DEFINE_string(protocol, "RStore", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group");
DEFINE_string(lock_manager, "1,1", "calvin lock manager");
DEFINE_string(partitioner, "hash", "database partitioner (hash, hash2, pb)");
DEFINE_bool(sleep_on_retry, true, "sleep when retry aborted transactions");
DEFINE_bool(read_on_replica, false, "read from replicas");
DEFINE_bool(local_validation, false, "local validation");
DEFINE_bool(rts_sync, false, "rts sync");
DEFINE_bool(kiva_read_only, true, "kiva read only optimization");
DEFINE_bool(kiva_reordering, true, "kiva reordering optimization");
DEFINE_bool(kiva_si, false, "kiva snapshot isolation");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(read_only_ratio, 80, "read only transaction ratio");
DEFINE_int32(delay, 0, "delay time in us.");
DEFINE_double(zipf, 0, "skew factor");
DEFINE_string(cdf_path, "", "path to cdf");
DEFINE_int32(keys, 200000, "keys in a partition.");
// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<std::string> peers;
  boost::algorithm::split(peers, FLAGS_servers, boost::is_any_of(";"));

  int n = FLAGS_threads;
  scar::retwis::Context context;
  context.coordinator_id = FLAGS_id;
  context.protocol = FLAGS_protocol;
  context.coordinator_num = peers.size();
  context.batch_size = FLAGS_batch_size;
  if (context.protocol == "kiva") {
    context.batch_size = context.batch_size / context.coordinator_num;
  }
  context.batch_flush = FLAGS_batch_flush;
  context.sleep_time = FLAGS_sleep_time;
  context.group_time = FLAGS_group_time;
  context.worker_num = n;
  context.partition_num = FLAGS_partition_num;
  context.replica_group = FLAGS_replica_group;
  context.lock_manager = FLAGS_lock_manager;
  context.partitioner = FLAGS_partitioner;
  context.sleep_on_retry = FLAGS_sleep_on_retry;
  context.read_on_replica = FLAGS_read_on_replica;
  context.local_validation = FLAGS_local_validation;
  context.rts_sync = FLAGS_rts_sync;
  context.kiva_read_only_optmization = FLAGS_kiva_read_only;
  context.kiva_reordering_optmization = FLAGS_kiva_reordering;
  context.kiva_snapshot_isolation = FLAGS_kiva_si;
  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.delay_time = FLAGS_delay;
  context.cdf_path = FLAGS_cdf_path;
  context.keysPerPartition = FLAGS_keys;

  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    scar::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
  }

  scar::retwis::Database db;
  db.initialize(context);

  auto c = std::make_unique<scar::Coordinator>(FLAGS_id, peers, db, context);
  c->connectToPeers();
  c->start();
  return 0;
}