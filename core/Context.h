//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <cstddef>

namespace scar {
class Context {
public:
  std::size_t partitionNum = 0;
  std::size_t workerNum = 0;
  std::size_t coordinatorNum = 0;

  bool retryAbortedTransaction_ = false;
  bool exponentialBackOff_ = false;
  bool readOnReplica_ = false;
  bool localValidation_ = false;
  bool syncReadTimestamp_ = false;
  bool operationReplication_ = false;
};
} // namespace scar
