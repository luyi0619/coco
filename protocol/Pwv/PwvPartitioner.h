//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "core/Partitioner.h"

namespace scar {
class PwvPartitioner : public Partitioner {

public:
  PwvPartitioner(std::size_t coordinator_id, std::size_t coordinator_num)
      : Partitioner(coordinator_id, coordinator_num) {
    CHECK(coordinator_num == 1) << "Pwv is currently a single node db.";
    CHECK(coordinator_id < coordinator_num);
  }

  ~PwvPartitioner() override = default;

  std::size_t replica_num() const override { return 1; }

  bool is_replicated() const override { return false; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_num;
  }

  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    return false;
  }

  bool is_backup() const override { return false; }
};

} // namespace scar