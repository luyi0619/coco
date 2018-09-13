//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/ycsb/Schema.h"

namespace scar {

  namespace ycsb {
    struct Storage {
      ycsb::key ycsb_keys[YCSB_FIELD_SIZE];
      ycsb::value ycsb_values[YCSB_FIELD_SIZE];
    };
  }
}