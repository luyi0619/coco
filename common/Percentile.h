//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include <algorithm>
#include <cmath>

#include <glog/logging.h>

// The nearest-rank method
// https://en.wikipedia.org/wiki/Percentile

namespace scar {

template <class T> class Percentile {
public:
  using element_type = T;

  void add(const element_type &value) {
    isSorted_ = false;
    data_.push_back(value);
  }

  void add(const std::vector<element_type> &v) {
    isSorted_ = false;
    std::copy(v.begin(), v.end(), std::back_inserter(data_));
  }

  void clear() {
    isSorted_ = true;
    data_.clear();
  }

  auto size() { return data_.size(); }

  element_type nth(double n) {
    checkSort();
    CHECK(n > 0 && n <= 100);
    auto sz = size();
    auto i = static_cast<decltype(sz)>(ceil(n / 100 * sz)) - 1;
    CHECK(i >= 0 && i < size());
    return data_[i];
  }

private:
  void checkSort() {
    if (!isSorted_) {
      std::sort(data_.begin(), data_.end());
      isSorted_ = true;
    }
  }

private:
  bool isSorted_ = true;
  std::vector<element_type> data_;
};
} // namespace scar