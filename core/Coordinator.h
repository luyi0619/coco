//
// Created by Yi Lu on 7/24/18.
//

#ifndef SCAR_COORDINATOR_H
#define SCAR_COORDINATOR_H

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

#endif // SCAR_COORDINATOR_H
