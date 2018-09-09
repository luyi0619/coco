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
  std::size_t batch_query = 240;

  virtual std::size_t get_s_phase_query_num() const = 0;
  virtual std::size_t get_c_phase_query_num() const = 0;

  bool retryAbortedTransaction_ = false;
  bool exponentialBackOff_ = false;
  bool readOnReplica_ = false;
  bool localValidation_ = false;
  bool syncReadTimestamp_ = false;
  bool operationReplication_ = false;
};
} // namespace scar
