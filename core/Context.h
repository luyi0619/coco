//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <cstddef>
#include <string>

namespace scar {
class Context {
public:
  std::size_t partition_num = 0;
  std::size_t worker_num = 0;
  std::size_t coordinator_num = 0;
  std::string protocol;
  std::string replica_group;
  std::string lock_manager;
  std::size_t batch_size = 240; // rstore or calvin batch size
  std::size_t batch_flush = 10;
  std::size_t group_time = 40; // ms
  std::size_t sleep_time = 50; // us
  std::string partitioner;
  std::size_t delay_time = 0;
  std::string cdf_path;

  bool read_on_replica = false;
  bool local_validation = false;
  bool rts_sync = false;
  bool sleep_on_retry = true;
  bool operation_replication = false;
};
} // namespace scar
