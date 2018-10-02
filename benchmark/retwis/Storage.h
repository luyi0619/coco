//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "benchmark/retwis/Schema.h"

namespace scar {

namespace retwis {
struct Storage {
  retwis::key keys[10];
  retwis::value values[10];
};

} // namespace retwis
} // namespace scar