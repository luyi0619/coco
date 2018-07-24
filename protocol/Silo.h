//
// Created by Yi Lu on 7/18/18.
//

#ifndef SCAR_SILO_H
#define SCAR_SILO_H

#include <atomic>
#include <glog/logging.h>
#include <thread>

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
  void set_key(void *key) { this->key = key; }

  void *get_key() const { return key; }

  // value
  void set_value(void *value) { this->value = value; }

  void *get_value() const { return value; }

  // sort_key

  void set_sort_key(void *sort_key) { this->sort_key = sort_key; }

  void *get_sort_key() const { return sort_key; }

private:
  /*   A bitvec is a 64-bit word. 16 bits for status bits. 16 bits for table id.
   * 32 bits for partition id. [ lock bit | unused bit (15) | table id (16) |
   * partition id (32) ] [     0    |    1 ... 15     |  16 ...  31   | 32... 63
   * ]
   */

  uint64_t bitvec = 0;
  uint64_t tid = 0;
  void *key = nullptr;
  void *value = nullptr;
  void *sort_key = nullptr;

private:
  static constexpr uint64_t __lock_bit_mask = 0x1ull;
  static constexpr uint64_t __table_id_mask = 0xffffull << 16;
  static constexpr uint64_t __partition_id_mask = 0xffffffffull << 32;
};

class Silo {
public:
  using DataType = std::atomic<uint64_t>;
  using RWKeyType = SiloRWKey;

  Silo(std::atomic<uint64_t> &epoch) : epoch(epoch) {}

  template <class ValueType>
  void read(std::tuple<DataType, ValueType> &row, ValueType &result) {
    DataType &tid = std::get<0>(row);
    ValueType &value = std::get<1>(row);
    uint64_t tid_;
    do {
      do {
        tid_ = tid.load();
      } while (isLocked(tid_));
      result = value;
    } while (tid_ != tid.load());
  }

  template <class DataType, class ValueType>
  void update(std::tuple<DataType, ValueType> &row, const ValueType &v) {
    DataType &tid = std::get<0>(row);
    ValueType &value = std::get<1>(row);
    uint64_t tid_ = tid.load();
    CHECK(isLocked(tid_));
    value = v;
  }

  bool commit(std::vector<SiloRWKey> &readSet,
              std::vector<SiloRWKey> &writeSet) {
    return true;
  }

private:
  bool isLocked(uint64_t value) { return value & LOCK_BIT_MASK; }

  uint64_t lock(std::atomic<uint64_t> &a) {
    uint64_t oldValue, newValue;
    do {
      do {
        oldValue = a.load();
      } while (isLocked(oldValue));
      newValue = oldValue | LOCK_BIT_MASK;
    } while (!a.compare_exchange_weak(oldValue, newValue));
    CHECK(isLocked(oldValue) == false);
    return oldValue;
  }

  uint64_t lock(std::atomic<uint64_t> &a, bool &success) {
    uint64_t oldValue = a.load();

    if (isLocked(oldValue)) {
      success = false;
    } else {
      uint64_t newValue = oldValue | LOCK_BIT_MASK;
      success = a.compare_exchange_strong(oldValue, newValue);
    }
    return oldValue;
  }

  void unlock(std::atomic<uint64_t> &a, uint64_t newValue) {
    uint64_t oldValue = a.load();
    CHECK(isLocked(oldValue));
    CHECK(isLocked(newValue) == false);
    bool ok = a.compare_exchange_strong(oldValue, newValue);
    CHECK(ok);
  }

  std::atomic<uint64_t> &epoch;
  uint64_t maxTID = 0;
  static constexpr uint64_t LOCK_BIT_MASK = 0x1ull << 63;
};

} // namespace scar

#endif // SCAR_SILO_H
