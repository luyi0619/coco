//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

namespace scar {

class SiloRWKey {
public:
  // lock_bit
  void set_lock_bit() { bitvec |= __lock_bit_mask; }

  void clear_lock_bit() { bitvec &= ~__lock_bit_mask; }

  bool is_lock_bit() const { return bitvec & __lock_bit_mask; }

  // table_id
  std::size_t get_table_id() const { return (bitvec & __table_id_mask) >> 16; }

  void set_table_id(std::size_t table_id) {
    CHECK(table_id < (1ull << 16));
    // clear table id
    bitvec &= ~__table_id_mask;
    bitvec |= table_id << 16;
  }

  // partition_id
  std::size_t get_partition_id() const {
    return (bitvec & __partition_id_mask) >> 32;
  }

  void set_partition_id(std::size_t partition_id) {
    CHECK(partition_id < (1ull << 32));
    // clear partition id
    bitvec &= ~__partition_id_mask;
    bitvec |= partition_id << 32;
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
  /*  A bitvec is a 64-bit word. 16 bits for status bits. 16 bits for table id.
   *  32 bits for partition id.
   *  [ lock bit | unused bit (15) | table id (16) | partition id (32) ]
   *  [     0    |    1 ... 15     |  16 ...  31   | 32... 63         ]
   */

  uint64_t bitvec = 0;
  uint64_t tid = 0;
  const void *key = nullptr;
  void *value = nullptr;
  const void *sort_key = nullptr;

private:
  static constexpr uint64_t __lock_bit_mask = 0x1ull;
  static constexpr uint64_t __table_id_mask = 0xffffull << 16;
  static constexpr uint64_t __partition_id_mask = 0xffffffffull << 32;
};
} // namespace scar
