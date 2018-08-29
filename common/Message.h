//
// Created by Yi Lu on 8/28/18.
//

#pragma once

#include "StringPiece.h"
#include <string>

namespace scar {

/*
 * [message size | 0xdeadbeef | message ... ]
 * It's the user's responsiblity to call flush(), which makes the size() equal
 * to the length of data
 *
 */

class Message {
public:
  using size_type = uint32_t;

  Message() : data(HEADER_SIZE, 0) {
    sizeRef() = 0;
    deadbeefRef() = DEADBEEF;
  }
  Message(const char *str, std::size_t len) : data(str, len) {}

  void clear() {
    data = std::string(HEADER_SIZE, 0);
    sizeRef() = 0;
    deadbeefRef() = DEADBEEF;
  }

  StringPiece toStringPiece() {
    return StringPiece(data.data() + HEADER_SIZE, data.size() - HEADER_SIZE);
  }

  void flush() {
    sizeRef() = data.size();
    deadbeefRef() = DEADBEEF;
  }

  bool checkSize() {
    auto sz = sizeRef();
    return sz == data.size();
  }

  bool checkDeadbeef() {
    auto deadbeef = deadbeefRef();
    return deadbeef == DEADBEEF;
  }

private:
  size_type &sizeRef() { return *reinterpret_cast<size_type *>(&data[0]); }

  uint32_t &deadbeefRef() {
    return *reinterpret_cast<uint32_t *>(&data[sizeof(size_type)]);
  }

public:
  std::string data;

  static constexpr int HEADER_SIZE = sizeof(size_type) + sizeof(uint32_t);
  static constexpr uint32_t DEADBEEF = 0xDEADBEEF;
};
} // namespace scar