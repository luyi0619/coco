#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");
DEFINE_string(skew_pattern, "both", "skew pattern: both, read, write");
DEFINE_bool(two_partitions, false, "dist transactions access two partitions.");

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  scar::ycsb::Context context;
  SETUP_CONTEXT(context);

  if (FLAGS_skew_pattern == "both") {
    context.skewPattern = scar::ycsb::YCSBSkewPattern::BOTH;
  } else if (FLAGS_skew_pattern == "read") {
    context.skewPattern = scar::ycsb::YCSBSkewPattern::READ;
  } else if (FLAGS_skew_pattern == "write") {
    context.skewPattern = scar::ycsb::YCSBSkewPattern::WRITE;
  } else {
    CHECK(false);
  }

  context.readWriteRatio = FLAGS_read_write_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.keysPerPartition = FLAGS_keys;
  context.two_partitions = FLAGS_two_partitions;

  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    scar::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
  }

  scar::ycsb::Database db;
  db.initialize(context);

  scar::Coordinator c(FLAGS_id, db, context);
  c.connectToPeers();
  c.start();
  return 0;
}