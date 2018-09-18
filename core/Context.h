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
  std::size_t batch_size = 240; // rstore or calvin batch size
  std::size_t batch_flush = 10;
  std::size_t group_time = 40; // ms
  std::string partitioner;

  bool operation_replication;

  virtual std::size_t get_s_phase_query_num() const = 0;
  virtual std::size_t get_c_phase_query_num() const = 0;
};
} // namespace scar
