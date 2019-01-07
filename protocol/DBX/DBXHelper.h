//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include <atomic>
#include <cstring>
#include <tuple>

#include "glog/logging.h"

namespace scar {

class DBXHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static uint64_t read(const std::tuple<MetaDataType *, void *> &row,
                       void *dest, std::size_t size) {
    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
    return tid.load();
  }

  static uint64_t get_epoch(uint64_t value) {
    return (value >> EPOCH_OFFSET) & EPOCH_MASK;
  }

  static uint64_t set_epoch(uint64_t value, uint64_t epoch) {
    DCHECK(epoch < (1ull << 24));
    return (value & (~(EPOCH_MASK << EPOCH_OFFSET))) | (epoch << EPOCH_OFFSET);
  }

  static uint64_t get_rts(uint64_t value) {
    return (value >> RTS_OFFSET) & RTS_MASK;
  }

  static uint64_t set_rts(uint64_t value, uint64_t rts) {
    DCHECK(rts < (1ull << 20));
    return (value & (~(RTS_MASK << RTS_OFFSET))) | (rts << RTS_OFFSET);
  }

  static uint64_t get_wts(uint64_t value) {
    return (value >> WTS_OFFSET) & WTS_MASK;
  }

  static uint64_t set_wts(uint64_t value, uint64_t wts) {
    DCHECK(wts < (1ull << 20));
    return (value & (~(WTS_MASK << WTS_OFFSET))) | (wts << WTS_OFFSET);
  }

public:
  /*
   * [epoch (24) | read-rts  (20) | write-wts (20)]
   *
   */

  static constexpr int EPOCH_OFFSET = 40;
  static constexpr uint64_t EPOCH_MASK = 0xffffffull;

  static constexpr int RTS_OFFSET = 20;
  static constexpr uint64_t RTS_MASK = 0xfffffull;

  static constexpr int WTS_OFFSET = 0;
  static constexpr uint64_t WTS_MASK = 0xfffffull;
};

} // namespace scar