//
// Created by Yi Lu on 9/17/18.
//

#pragma once

#include "common/Encoder.h"
#include <string>

namespace scar {

class Operation {

public:
  Operation() {}

  Operation(uint32_t table_id, uint32_t partition_id, uint32_t operation_type) {
    set_table_id(table_id);
    set_partition_id(partition_id);
    set_operation_type(operation_type);
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

  // operation type

  void set_operation_type(uint32_t operation_type) {
    DCHECK(operation_type < (1 << 19));
    clear_operation_type();
    bitvec |= operation_type << OPERATION_TYPE_OFFSET;
  }

  void clear_operation_type() {
    bitvec &= ~(OPERATION_TYPE_MASK << OPERATION_TYPE_OFFSET);
  }

  uint32_t get_operation_type() const {
    return (bitvec >> OPERATION_TYPE_OFFSET) & OPERATION_TYPE_MASK;
  }

  void set_tid(uint64_t id) { tid = id; }

  uint64_t get_tid() const { return tid; }

public:
  /*
   * A bitvec is a 32-bit word.
   *
   * [ table id (5) ] | partition id (8) | operation type (19) |
   *
   */

  uint32_t bitvec;
  uint64_t tid;
  std::string data;

  static constexpr uint32_t TABLE_ID_MASK = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 27;

  static constexpr uint32_t PARTITION_ID_MASK = 0xff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 19;

  static constexpr uint32_t OPERATION_TYPE_MASK = 0x7ffff;
  static constexpr uint32_t OPERATION_TYPE_OFFSET = 0;
};
} // namespace scar