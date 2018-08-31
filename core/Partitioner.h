//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <glog/logging.h>

namespace scar {

class Partitioner {
public:
  Partitioner(std::size_t coordinator_id, std::size_t coordinator_nums) {
    CHECK(coordinator_id < coordinator_nums);
    this->coordinator_id = coordinator_id;
    this->coordinator_nums = coordinator_nums;
  }

  std::size_t total_coordinators() const { return coordinator_nums; }

  virtual bool is_replicated() const = 0;

  virtual bool has_master_partition(std::size_t partition_id) const = 0;

  virtual std::size_t master_coordinator(std::size_t partition_id) const = 0;

  virtual bool is_partition_replicated_on(std::size_t partition_id,
                                          std::size_t coordinator_id) const = 0;

protected:
  std::size_t coordinator_id;
  std::size_t coordinator_nums;
};

/*
 * N is the total number of replicas.
 * N is always larger than 0.
 * The N coordinators from the master coordinator have the replication for a
 * given partition.
 */

template <std::size_t N> class HashReplicatedPartitioner : public Partitioner {
public:
  HashReplicatedPartitioner(std::size_t coordinator_id,
                            std::size_t coordinator_nums)
      : Partitioner(coordinator_id, coordinator_nums) {
    CHECK(N > 0 && N <= coordinator_nums);
  }

  bool is_replicated() const override { return N > 1; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_nums;
  }

  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    CHECK(coordinator_id < coordinator_nums);
    std::size_t first_replica = master_coordinator(partition_id);
    std::size_t last_replica = (first_replica + N - 1) % coordinator_nums;

    if (last_replica >= first_replica) {
      return first_replica <= coordinator_id && coordinator_id <= last_replica;
    } else {
      return coordinator_id >= first_replica || coordinator_id <= last_replica;
    }
  }
};

using HashPartitioner = HashReplicatedPartitioner<1>;

/*
 * There are 2 replicas in the system with N coordinators.
 * Coordinator 0 has a full replica.
 * The other replica is partitioned across coordinator 1 and coordinator N - 1
 *
 *
 * The master partition is partition id % N.
 *
 * case 1
 * If the master partition is from coordinator 1 to coordinator N - 1,
 * the secondary partition is on coordinator 0.
 *
 * case 2
 * If the master partition is on coordinator 0,
 * the secondary partition is from coordinator 1 to coordinator N - 1.
 *
 */

class RackDBPartitioner : public Partitioner {
public:
  RackDBPartitioner(std::size_t coordinator_id, std::size_t coordinator_nums)
      : Partitioner(coordinator_id, coordinator_nums) {
    CHECK(coordinator_nums >= 2);
  }

  bool is_replicated() const override { return true; }

  bool has_master_partition(std::size_t partition_id) const override {
    return master_coordinator(partition_id) == coordinator_id;
  }

  std::size_t master_coordinator(std::size_t partition_id) const override {
    return partition_id % coordinator_nums;
  }

  bool is_partition_replicated_on(std::size_t partition_id,
                                  std::size_t coordinator_id) const override {
    CHECK(coordinator_id < coordinator_nums);

    auto master_id = master_coordinator(partition_id);
    auto secondary_id = 0u; // case 1
    if (master_id == 0) {
      secondary_id = partition_id % (coordinator_nums - 1) + 1; // case 2
    }
    return coordinator_id == master_id || coordinator_id == secondary_id;
  }
};

} // namespace scar