//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include <string>
#include <vector>

#include "common/Random.h"

namespace scar {
namespace retwis {
class Random : public scar::Random {
public:
  using scar::Random::Random;

  std::string rand_str(std::size_t length) {
    auto &characters_ = characters();
    auto characters_len = characters_.length();
    std::string result;
    for (auto i = 0u; i < length; i++) {
      int k = uniform_dist(0, characters_len - 1);
      result += characters_[k];
    }
    return result;
  }

private:
  static const std::string &characters() {
    static std::string characters_ =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return characters_;
  };
};
} // namespace retwis
} // namespace scar