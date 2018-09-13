//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include <glog/logging.h>
#include <numeric>
#include <vector>

namespace scar {

/*
 *
 *                         replica group 0
 *
 *         coordinator 0                        coordinator 1
 *
 *      shard 0        shard 2           shard 1          shard 3
 *  ___________      ___________       ___________      ___________
 * |     |     |    |     |     |     |     |     |    |     |     |
 * |par 0|par 4|    |par 2|par 6|     |par 1|par 5|    |par 3|par 7|
 * |_____|_____|    |_____|_____|     |_____|_____|    |_____|_____|
 *
 *
 *                            replica group 1
 *
 *                             coordinator 2
 *
 *               shard 0               shard 1             shard 2
 *        _________________      _________________       ___________
 *       |     |     |     |    |     |     |     |     |     |     |
 *       |par 0|par 3|par 6|    |par 1|par 4|par 7|     |par 2|par 5|
 *       |_____|_____|_____|    |_____|_____|_____|     |_____|_____|
 *
 */

class CalvinPartitioner {

public:
  CalvinPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                    std::size_t shard_num,
                    std::vector<std::size_t> replica_group_sizes)
      : coordinator_id(coordinator_id), coordinator_num(coordinator_num),
        shard_num(shard_num) {

    std::size_t size = 0;
    for (auto i = 0u; i < replica_group_sizes.size(); i++) {
      DCHECK(replica_group_sizes[i] > 0);
      size += replica_group_sizes[i];

      if (coordinator_id < size) {
        coordinator_start_id = size - replica_group_sizes[i];
        replica_group_id = i;
        replica_group_size = replica_group_sizes[i];
        break;
      }
    }
    DCHECK(std::accumulate(replica_group_sizes.begin(),
                           replica_group_sizes.end(), 0) == coordinator_num);
  }

  std::size_t get_shard_id(std::size_t partition_id) {
    return partition_id % shard_num;
  }

  std::size_t master_coordinator(std::size_t shard_id) {
    DCHECK(shard_id < shard_num);
    return shard_id % replica_group_size + coordinator_start_id;
  }

  bool has_master_shard(std::size_t shard_id) {
    DCHECK(shard_id < shard_num);
    return master_coordinator(shard_id) == coordinator_id;
  }

public:
  std::size_t coordinator_id;
  std::size_t coordinator_num;
  std::size_t shard_num;
  std::size_t replica_group_id;
  std::size_t replica_group_size;

private:
  // the first coordinator in this replica group
  std::size_t coordinator_start_id;
};

} // namespace scar