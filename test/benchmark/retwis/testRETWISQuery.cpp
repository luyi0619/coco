//
// Created by Yi Lu on 10/1/18.
//

#include "benchmark/retwis/Query.h"
#include <gtest/gtest.h>

TEST(TestRETWISQuery, TestBasic) {
  scar::retwis::Context context;
  context.strategy = scar::retwis::PartitionStrategy::ROUND_ROBIN;
  context.keysPerPartition = 20;
  context.crossPartitionProbability = 50;
  context.isUniform = true;

  context.partition_num = 10;
  context.worker_num = 10;

  scar::retwis::Random random(reinterpret_cast<uint64_t>(&context));

  constexpr int partitionID = 0;
  scar::retwis::AddUserQuery q1 =
      scar::retwis::makeAddUserQuery()(context, partitionID, random);
  scar::retwis::FollowUnfollowQuery q2 =
      scar::retwis::makeFollowUnfollowQuery()(context, partitionID, random);
  scar::retwis::PostTweetQuery q3 =
      scar::retwis::makePostTweetQuery()(context, partitionID, random);
  scar::retwis::GetTimelineQuery q4 =
      scar::retwis::makeGetTimelineQuery()(context, partitionID, random);
}