//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <glog/logging.h>

namespace scar {

class TwoPLHelper {
public:
  using MetaDataType = std::atomic<uint64_t>;

  static uint64_t read(const std::tuple<MetaDataType *, void *> &row,
                       void *dest, std::size_t size) {

    MetaDataType &tid = *std::get<0>(row);
    void *src = std::get<1>(row);
    std::memcpy(dest, src, size);
    uint64_t tid_ = tid.load();
    return remove_write_lock_bit(remove_read_lock_bit(tid_));
  }

  /**
   * [write lock bit (1) |  read lock bit (9) -- 512 - 1 locks | seq id  (54) ]
   *
   */

  static bool is_read_locked(uint64_t value) {
    return value & (READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static bool is_write_locked(uint64_t value) {
    return value & (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

  static uint64_t read_lock_num(uint64_t value) {
    return (value >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK;
  }

  static uint64_t read_lock_max() { return READ_LOCK_BIT_MASK; }

  static bool read_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    old_value = a.load();
    if (is_write_locked(old_value) ||
        read_lock_num(old_value) == read_lock_max()) {
      return false;
    }
    new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
    return a.compare_exchange_strong(old_value, new_value);
  }

  static bool write_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value = a.load();
    if (is_read_locked(old_value) || is_write_locked(old_value)) {
      return false;
    }
    uint64_t new_value =
        old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
    return a.compare_exchange_strong(old_value, new_value);
  }

  static void read_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    old_value = a.load();
    DCHECK(is_read_locked(old_value));
    DCHECK(!is_write_locked(old_value));
    new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
    a.compare_exchange_weak(old_value, new_value);
  }

  static void write_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    old_value = a.load();
    DCHECK(!is_read_locked(old_value));
    DCHECK(is_write_locked(old_value));
    new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
    a.compare_exchange_weak(old_value, new_value);
  }

  static uint64_t remove_read_lock_bit(uint64_t value) {
    return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static uint64_t remove_write_lock_bit(uint64_t value) {
    return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

public:
  static constexpr int READ_LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

  static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;
};
} // namespace scar