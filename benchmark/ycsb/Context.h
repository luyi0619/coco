//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

#include <glog/logging.h>

namespace scar {
namespace ycsb {

enum class PartitionStrategy { RANGE, ROUND_ROBIN };

class Context : public scar::Context {
public:
  std::size_t getPartitionID(std::size_t key) const {
    CHECK(key >= 0 && key < partitionNum * keysPerPartition);

    if (strategy == PartitionStrategy::ROUND_ROBIN) {
      return key % partitionNum;
    } else {
      return key / keysPerPartition;
    }
  }

  std::size_t getGlobalKeyID(std::size_t key, std::size_t partitionID) const {
    CHECK(key >= 0 && key < keysPerPartition && partitionID >= 0 &&
          partitionID < partitionNum);

    if (strategy == PartitionStrategy::ROUND_ROBIN) {
      return key * partitionNum + partitionID;
    } else {
      return partitionID * keysPerPartition + key;
    }
  }

public:
  int readWriteRatio = 0;            // out of 100
  int readOnlyTransaction = 0;       //  out of 100
  int crossPartitionProbability = 0; // out of 100

  std::size_t keysPerTransaction = 10;
  std::size_t keysPerPartition = 200000;

  bool isUniform = true;

  PartitionStrategy strategy = PartitionStrategy::ROUND_ROBIN;
};
} // namespace ycsb
} // namespace scar
