//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace scar {
class Context {
public:
  std::size_t coordinator_id = 0;
  std::size_t partition_num = 0;
  std::size_t worker_num = 0;
  std::size_t coordinator_num = 0;
  std::size_t io_thread_num = 1;
  std::string protocol;
  std::string replica_group;
  std::string lock_manager;
  std::size_t batch_size = 240; // rstore, calvin, dbx batch size
  std::size_t batch_flush = 10;
  std::size_t group_time = 40; // ms
  std::size_t sleep_time = 50; // us
  std::string partitioner;
  std::size_t delay_time = 0;
  std::string log_path;
  std::string cdf_path;
  std::size_t cpu_core_id = 0;

  bool tcp_no_delay = true;
  bool tcp_quick_ack = false;

  bool cpu_affinity = true;

  bool sleep_on_retry = true;

  bool read_on_replica = false;
  bool local_validation = false;
  bool rts_sync = false;
  bool parallel_locking_and_validation = true;

  bool calvin_same_batch = false;

  bool kiva_read_only_optmization = true;
  bool kiva_reordering_optmization = true;
  bool kiva_snapshot_isolation = false;
  bool operation_replication = false;

  std::vector<std::string> peers;
};
} // namespace scar
