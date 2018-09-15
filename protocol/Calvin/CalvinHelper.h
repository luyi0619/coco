//
// Created by Yi Lu on 9/15/18.
//

#pragma once

#include <boost/algorithm/string.hpp>
#include <string>
#include <vector>

#include <glog/logging.h>

namespace scar {

class CalvinHelper {

public:
  static std::vector<std::size_t>
  get_replica_group_sizes(const std::string &replica_group) {
    std::vector<std::string> replica_group_sizes_string;
    boost::algorithm::split(replica_group_sizes_string, replica_group,
                            boost::is_any_of(","));
    std::vector<std::size_t> replica_group_sizes;
    for (auto i = 0u; i < replica_group_sizes_string.size(); i++) {
      replica_group_sizes.push_back(
          std::atoi(replica_group_sizes_string[i].c_str()));
    }

    return replica_group_sizes;
  }

  /**
   *
   * The following code is adapted from TwoPLHelper.h
   * For Calvin, we can use lower 63 bits for read locks.
   * However, 511 locks are enough and the code above is well tested.
   *
   * [write lock bit (1) |  read lock bit (9) -- 512 - 1 locks ]
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

  static uint64_t read_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      do {
        old_value = a.load();
      } while (is_write_locked(old_value) ||
               read_lock_num(old_value) == read_lock_max());
      new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static uint64_t write_lock(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;

    do {
      do {
        old_value = a.load();
      } while (is_read_locked(old_value) || is_write_locked(old_value));

      new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);

    } while (!a.compare_exchange_weak(old_value, new_value));
    return remove_lock_bit(old_value);
  }

  static void read_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    do {
      old_value = a.load();
      DCHECK(is_read_locked(old_value));
      DCHECK(!is_write_locked(old_value));
      new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
    } while (!a.compare_exchange_weak(old_value, new_value));
  }

  static void write_lock_release(std::atomic<uint64_t> &a) {
    uint64_t old_value, new_value;
    old_value = a.load();
    DCHECK(!is_read_locked(old_value));
    DCHECK(is_write_locked(old_value));
    new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
    bool ok = a.compare_exchange_strong(old_value, new_value);
    DCHECK(ok);
  }

  static uint64_t remove_lock_bit(uint64_t value) {
    return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
  }

  static uint64_t remove_read_lock_bit(uint64_t value) {
    return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
  }

  static uint64_t remove_write_lock_bit(uint64_t value) {
    return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
  }

public:
  static constexpr int LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

  static constexpr int READ_LOCK_BIT_OFFSET = 54;
  static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

  static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;
};
} // namespace scar