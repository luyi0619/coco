//
// Created by Yi Lu on 9/3/18.
//

#pragma once

#include <atomic>

#include "glog/logging.h"

namespace scar {

class SiloHelper {

public:
  using MetaDataType = std::atomic<uint64_t>;

  static uint64_t read(const std::tuple<MetaDataType *, void *> &row,
                       void *dest, std::size_t size) {

    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);

    // read from a consistent view. read the value even it's locked by others.
    // abort in read validation phase
    uint64_t tid_;
    do {
      tid_ = tid.load();
      std::memcpy(dest, src, size);
    } while (tid_ != tid.load());

    return SiloHelper::removeLockBit(tid_);
  }

  static bool isLocked(uint64_t value) {
    return (value >> LOCK_BIT_OFFSET) & LOCK_BIT_MASK;
  }

  static uint64_t lock(std::atomic<uint64_t> &a) {
    uint64_t oldValue, newValue;
    do {
      do {
        oldValue = a.load();
      } while (isLocked(oldValue));
      newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
    } while (!a.compare_exchange_weak(oldValue, newValue));
    CHECK(isLocked(oldValue) == false);
    return oldValue;
  }

  static uint64_t lock(std::atomic<uint64_t> &a, bool &success) {
    uint64_t oldValue = a.load();

    if (isLocked(oldValue)) {
      success = false;
    } else {
      uint64_t newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
      success = a.compare_exchange_strong(oldValue, newValue);
    }
    return oldValue;
  }

  static void unlock(std::atomic<uint64_t> &a) {
    uint64_t oldValue = a.load();
    CHECK(isLocked(oldValue));
    uint64_t newValue = removeLockBit(oldValue);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    CHECK(ok);
  }

  static void unlock(std::atomic<uint64_t> &a, uint64_t newValue) {
    uint64_t oldValue = a.load();
    CHECK(isLocked(oldValue));
    CHECK(isLocked(newValue) == false);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    CHECK(ok);
  }

  static uint64_t removeLockBit(uint64_t value) {
    return (~(LOCK_BIT_MASK << LOCK_BIT_OFFSET)) & value;
  }

  static uint64_t getEpoch(uint64_t value) {
    return (value & (SILO_EPOCH_MASK << SILO_EPOCH_OFFSET)) >>
           SILO_EPOCH_OFFSET;
  }

public:
  static constexpr int SILO_EPOCH_OFFSET = 30;
  static constexpr uint64_t SILO_EPOCH_MASK = 0x1ffffffffull;
  static constexpr int LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t LOCK_BIT_MASK = 0x1ull;
};

} // namespace scar