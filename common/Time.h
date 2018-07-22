//
// Created by Yi Lu on 7/22/18.
//

#ifndef SCAR_TIME_H
#define SCAR_TIME_H

#include <chrono>

namespace scar {

class Time {
public:
  static uint64_t now() {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now - startTime)
        .count();
  }

  static std::chrono::steady_clock::time_point startTime;
};

} // namespace scar

#endif // SCAR_TIME_H