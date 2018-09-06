//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

namespace scar {

class RStoreRWKey {
public:
  // lock bit
  void set_lock_bit() {
    clear_lock_bit();
    bitvec |= LOCK_BIT_MASK << LOCK_BIT_OFFSET;
  }

  void clear_lock_bit() { bitvec &= ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET); }

  bool get_lock_bit() const {
    return (bitvec >> LOCK_BIT_OFFSET) & LOCK_BIT_MASK;
  }

  // table id

  void set_table_id(uint32_t table_id) {
    DCHECK(table_id < (1 << 5));
    clear_table_id();
    bitvec |= table_id << TABLE_ID_OFFSET;
  }

  void clear_table_id() { bitvec &= ~(TABLE_ID_MASK << TABLE_ID_OFFSET); }

  uint32_t get_table_id() const {
    return (bitvec >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
  }
  // partition id

  void set_partition_id(uint32_t partition_id) {
    DCHECK(partition_id < (1 << 8));
    clear_partition_id();
    bitvec |= partition_id << PARTITION_ID_OFFSET;
  }

  void clear_partition_id() {
    bitvec &= ~(PARTITION_ID_MASK << PARTITION_ID_OFFSET);
  }

  uint32_t get_partition_id() const {
    return (bitvec >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
  }

  // tid
  uint64_t get_tid() const { return tid; }

  void set_tid(uint64_t tid) { this->tid = tid; }

  // key
  void set_key(const void *key) { this->key = key; }

  const void *get_key() const { return key; }

  // value
  void set_value(void *value) { this->value = value; }

  void *get_value() const { return value; }

  // sort_key

  void set_sort_key(const void *sort_key) { this->sort_key = sort_key; }

  const void *get_sort_key() const { return sort_key; }

private:
  /*
   * A bitvec is a 32-bit word.
   *
   * [ table id (5) ] | partition id (8) | unused bit (18) | lock bit(1) ]
   *
   */

  uint64_t bitvec = 0;
  uint64_t tid = 0;
  const void *key = nullptr;
  void *value = nullptr;
  const void *sort_key = nullptr;

public:
  static constexpr uint32_t TABLE_ID_MASK = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 27;
  static constexpr uint32_t PARTITION_ID_MASK = 0xff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 19;
  static constexpr uint32_t LOCK_BIT_MASK = 0x1;
  static constexpr uint32_t LOCK_BIT_OFFSET = 0;
};
} // namespace scar
