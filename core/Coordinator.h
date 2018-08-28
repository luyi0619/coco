//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include <vector>

#include "core/Worker.h"

namespace scar {

template <class Workload> class Coordinator {
public:
  void start() {}

private:
  Database db;
  std::vector<std::unique_ptr<Worker<Workload>>> workers;
};
} // namespace scar

