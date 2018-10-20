//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "benchmark/retwis/Context.h"
#include "benchmark/retwis/Random.h"
#include "common/Zipf.h"

namespace scar {
namespace retwis {

class makeNRandomKey {
public:
  template <std::size_t N>
  void operator()(const Context &context, uint32_t partitionID, Random &random,
                  int32_t (&keys)[N], bool write) {

    int crossPartition = random.uniform_dist(1, 100);

    for (auto i = 0u; i < N; i++) {

      int32_t key;

      // generate a key in a partition
      bool retry;
      do {
        retry = false;
        if (write || context.isUniform) {
          key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition) - 1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }

        if (crossPartition <= context.crossPartitionProbability &&
            context.partition_num > 1) {
          auto newPartitionID = partitionID;
          while (newPartitionID == partitionID) {
            newPartitionID = random.uniform_dist(0, context.partition_num - 1);
          }
          keys[i] = context.getGlobalKeyID(key, newPartitionID);
        } else {
          keys[i] = context.getGlobalKeyID(key, partitionID);
        }

        for (auto k = 0u; k < i; k++) {
          if (keys[k] == keys[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);
    }
  }
};

struct AddUserQuery {
  int32_t KEY[3];
};

class makeAddUserQuery {
public:
  AddUserQuery operator()(const Context &context, uint32_t partitionID,
                          Random &random) const {
    AddUserQuery query;
    makeNRandomKey()(context, partitionID, random, query.KEY, true);
    return query;
  }
};

struct FollowUnfollowQuery {
  int32_t KEY[2];
};

class makeFollowUnfollowQuery {
public:
  FollowUnfollowQuery operator()(const Context &context, uint32_t partitionID,
                                 Random &random) const {
    FollowUnfollowQuery query;
    makeNRandomKey()(context, partitionID, random, query.KEY, true);
    return query;
  }
};

struct PostTweetQuery {
  int32_t KEY[5];
};

class makePostTweetQuery {
public:
  PostTweetQuery operator()(const Context &context, uint32_t partitionID,
                            Random &random) const {
    PostTweetQuery query;
    makeNRandomKey()(context, partitionID, random, query.KEY, true);
    return query;
  }
};

struct GetTimelineQuery {
  int32_t N;
  int32_t KEY[10];
};

class makeGetTimelineQuery {
public:
  GetTimelineQuery operator()(const Context &context, uint32_t partitionID,
                              Random &random) const {
    GetTimelineQuery query;
    query.N = random.uniform_dist(1, 10);
    makeNRandomKey()(context, partitionID, random, query.KEY, false);
    return query;
  }
};

} // namespace retwis
} // namespace scar