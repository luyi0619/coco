//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"

namespace scar {
namespace ycsb {

template <std::size_t N> struct YCSBQuery {
  int32_t Y_KEY[N];
  bool UPDATE[N];
};

template <std::size_t N> class makeYCSBQuery {
public:
  YCSBQuery<N> operator()(const Context &context, uint32_t partitionID,
                          Random &random) const {

    YCSBQuery<N> query;
    int readOnly = random.uniform_dist(1, 100);
    for (auto i = 0; i < N; i++) {

      int32_t key;

      // generate a key in a partition

      bool retry;
      do {
        retry = false;

        if (context.isUniform) {
          key = random.uniform_dist(
              0, static_cast<int>(context.keysPerPartition) - 1);
        } else {
          key = Zipf::globalZipf().value(random.next_double());
        }

        int crossPartition = random.uniform_dist(1, 100);

        if (crossPartition <= context.crossPartitionProbability &&
            context.partitionNum > 1) {
          int newPartitionID = partitionID;
          while (newPartitionID == partitionID) {
            newPartitionID = random.uniform_dist(0, context.partitionNum - 1);
          }
          query.Y_KEY[i] = context.getGlobalKeyID(key, newPartitionID);
        } else {
          query.Y_KEY[i] = context.getGlobalKeyID(key, partitionID);
        }

        for (auto k = 0u; k < i; k++) {
          if (query.Y_KEY[k] == query.Y_KEY[i]) {
            retry = true;
            break;
          }
        }
      } while (retry);

      // read or write

      if (readOnly <= context.readOnlyTransaction) {
        query.UPDATE[i] = false;
      } else {
        int readOrWrite = random.uniform_dist(1, 100);
        if (readOrWrite <= context.readWriteRatio) {
          query.UPDATE[i] = false;
        } else {
          query.UPDATE[i] = true;
        }
      }
    }
    return query;
  }
};
} // namespace ycsb
} // namespace scar

