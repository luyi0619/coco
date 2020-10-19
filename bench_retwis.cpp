//
// Created by Yi Lu on 10/1/18.
//

#include "benchmark/retwis/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(read_only_ratio, 80, "read only transaction ratio");
DEFINE_double(zipf, 0, "skew factor");

int main(int argc, char *argv[]) {

  google::InitGoogleLogging(argv[0]);
  google::InstallFailureSignalHandler();
  google::ParseCommandLineFlags(&argc, &argv, true);

  coco::retwis::Context context;
  SETUP_CONTEXT(context);

  context.crossPartitionProbability = FLAGS_cross_ratio;
  context.readOnlyTransaction = FLAGS_read_only_ratio;
  context.keysPerPartition = FLAGS_keys;

  if (FLAGS_zipf > 0) {
    context.isUniform = false;
    coco::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
  }

  coco::retwis::Database db;
  db.initialize(context);

  coco::Coordinator c(FLAGS_id, db, context);
  c.connectToPeers();
  c.start();
  return 0;
}