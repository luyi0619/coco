//
// Created by Yi Lu on 9/19/18.
//

#pragma once

#include <atomic>
#include <cstring>
#include <tuple>

#include "glog/logging.h"

namespace scar {

class ScarHelper {

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

    return remove_lock_bit(tid_);
  }

  static bool validate_read_key(std::atomic<uint64_t> &latest_tid, uint64_t tid,
                                uint64_t commit_ts, uint64_t &written_ts) {

    uint64_t rts = get_rts(tid);

    if (rts >= commit_ts) {
      return true;
    }

    bool success;

    do {
      success = true;

      uint64_t v1 = latest_tid.load();

      if (get_wts(tid) != get_wts(v1)) {
        return false;
      }

      // must be locked by others

      if (get_rts(v1) < commit_ts && is_locked(v1)) {
        return false;
      }

      // extend the rts of the tuple

      if (get_rts(v1) < commit_ts) {
        uint64_t v2 = set_rts(v1, commit_ts); // old_ts, new_ts
        success = latest_tid.compare_exchange_weak(v1, v2);
        if (success) {
          written_ts = remove_lock_bit(v2);
        }
      }
    } while (!success);

    DCHECK(is_locked(written_ts) == false);

    return success;
  }

  static bool is_locked(uint64_t value) {
    return (value >> LOCK_BIT_OFFSET) & LOCK_BIT_MASK;
  }

  static uint64_t lock(std::atomic<uint64_t> &a) {
    uint64_t oldValue, newValue;
    do {
      do {
        oldValue = a.load();
      } while (is_locked(oldValue));
      newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
    } while (!a.compare_exchange_weak(oldValue, newValue));
    DCHECK(is_locked(oldValue) == false);
    return oldValue;
  }

  static uint64_t lock(std::atomic<uint64_t> &a, bool &success) {
    uint64_t oldValue = a.load();

    if (is_locked(oldValue)) {
      success = false;
    } else {
      uint64_t newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
      success = a.compare_exchange_strong(oldValue, newValue);
    }
    return oldValue;
  }

  static void unlock(std::atomic<uint64_t> &a) {
    uint64_t oldValue = a.load();
    DCHECK(is_locked(oldValue));
    uint64_t newValue = remove_lock_bit(oldValue);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    DCHECK(ok);
  }

  static void unlock(std::atomic<uint64_t> &a, uint64_t newValue) {
    uint64_t oldValue = a.load();
    DCHECK(is_locked(oldValue));
    DCHECK(is_locked(newValue) == false);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    DCHECK(ok);
  }

  static uint64_t remove_lock_bit(uint64_t value) {
    return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
  }

  static uint64_t get_wts(uint64_t value) {
    return (value >> WTS_OFFSET) & WTS_MASK;
  }

  static uint64_t set_wts(uint64_t value, uint64_t wts) {
    DCHECK(wts < (1ull << 48));
    return (value & (~(WTS_MASK << WTS_OFFSET))) | (wts << WTS_OFFSET);
  }

  static uint64_t get_delta(uint64_t value) {
    return (value >> DELTA_OFFSET) & DELTA_MASK;
  }

  static uint64_t set_delta(uint64_t value, uint64_t delta) {
    DCHECK(delta < (1ull << 15));
    return (value & (~(DELTA_MASK << DELTA_OFFSET))) | (delta << DELTA_OFFSET);
  }

  static uint64_t get_rts(uint64_t value) {
    return get_wts(value) + get_delta(value);
  }

  static uint64_t set_rts(uint64_t value, uint64_t rts) {
    // handle data overflow

    DCHECK(rts >= get_wts(value));

    uint64_t delta = rts - get_wts(value);
    uint64_t shift = delta - (delta & DELTA_MASK);

    uint64_t v = value;

    v = set_wts(v, get_wts(v) + shift);
    v = set_delta(v, delta - shift);

    return v;
  }

public:
  static constexpr int LOCK_BIT_OFFSET = 63;
  static constexpr uint64_t LOCK_BIT_MASK = 0x1ull;

  static constexpr int DELTA_OFFSET = 48;
  static constexpr uint64_t DELTA_MASK = 0x7fffull;

  static constexpr int WTS_OFFSET = 0;
  static constexpr uint64_t WTS_MASK = 0xffffffffffffull;
};

} // namespace scar