//
// Created by Yi Lu on 8/30/18.
//

#pragma once

#include "common/StringPiece.h"

namespace scar {

/*
 * MessagePiece header format
 *
 * | Message type (7 => 128) | Message length (12 => 4096) | table id (5 => 32)
 * | partition id (8 => 256) |
 *
 */

class MessagePiece {

public:
  using header_type = uint32_t;

  MessagePiece(const MessagePiece &messagePiece)
      : stringPiece(messagePiece.stringPiece) {}

  MessagePiece(const StringPiece &stringPiece) : stringPiece(stringPiece) {}

  uint32_t get_message_type() {
    return (get_header_ref() >> MESSAGE_TYPE_OFFSET) & MESSAGE_TYPE_MASK;
  }

  uint32_t get_message_length() {
    return (get_header_ref() >> MESSAGE_LENGTH_OFFSET) & MESSAGE_LENGTH_MASK;
  }

  uint32_t get_table_id() {
    return (get_header_ref() >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
  }

  uint32_t get_partition_id() {
    return (get_header_ref() >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
  }

  StringPiece toStringPiece() {
    return StringPiece(stringPiece.data() + sizeof(header_type),
                       get_message_length());
  }

private:
  uint32_t get_header_ref() {
    return *reinterpret_cast<const uint32_t *>(stringPiece.data());
  }

public:
  StringPiece stringPiece;

public:
  static uint32_t construct_message_piece_header(uint32_t message_type,
                                                 uint32_t message_length,
                                                 std::size_t table_id,
                                                 std::size_t partition_id) {
    CHECK(message_type < (1u << 7));
    CHECK(message_length < (1u << 12));
    CHECK(table_id < (1u << 5));
    CHECK(partition_id < (1u << 8));

    return (message_type << MESSAGE_TYPE_OFFSET) +
           (message_length << MESSAGE_LENGTH_OFFSET) +
           (table_id << TABLE_ID_OFFSET) +
           (partition_id << PARTITION_ID_OFFSET);
  }

public:
  static constexpr uint32_t MESSAGE_TYPE_MASK = 0x7f;
  static constexpr uint32_t MESSAGE_TYPE_OFFSET = 25;
  static constexpr uint32_t MESSAGE_LENGTH_MASK = 0xfff;
  static constexpr uint32_t MESSAGE_LENGTH_OFFSET = 13;
  static constexpr uint32_t TABLE_ID_MASK = 0x1f;
  static constexpr uint32_t TABLE_ID_OFFSET = 8;
  static constexpr uint32_t PARTITION_ID_MASK = 0xff;
  static constexpr uint32_t PARTITION_ID_OFFSET = 0;
};
} // namespace scar
