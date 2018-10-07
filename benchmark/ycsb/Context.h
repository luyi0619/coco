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
  std::size_t getPartitionID(int32_t key) const {
    DCHECK(key >= 0 && key < partition_num * keysPerPartition);

    if (strategy == PartitionStrategy::ROUND_ROBIN) {
      return key % partition_num;
    } else {
      return key / keysPerPartition;
    }
  }

  int32_t getGlobalKeyID(int32_t key, std::size_t partitionID) const {
    DCHECK(key >= 0 && key < keysPerPartition && partitionID >= 0 &&
           partitionID < partition_num);

    if (strategy == PartitionStrategy::ROUND_ROBIN) {
      return key * partition_num + partitionID;
    } else {
      return partitionID * keysPerPartition + key;
    }
  }

  Context get_single_partition_context() const {
    Context c = *this;
    c.crossPartitionProbability = 0;
    c.operation_replication = this->operation_replication;
    return c;
  }

  Context get_cross_partition_context() const {
    Context c = *this;
    c.crossPartitionProbability = 100;
    c.operation_replication = false;
    return c;
  }

public:
  int readWriteRatio = 0;            // out of 100
  int readOnlyTransaction = 0;       //  out of 100
  int crossPartitionProbability = 0; // out of 100

  std::size_t keysPerTransaction = 4;
  std::size_t keysPerPartition = 200000;

  bool isUniform = true;

  PartitionStrategy strategy = PartitionStrategy::ROUND_ROBIN;
};
} // namespace ycsb
} // namespace scar
