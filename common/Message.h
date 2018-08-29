//
// Created by Yi Lu on 8/28/18.
//

#pragma once

#include "StringPiece.h"
#include <string>
namespace scar {
class Message {
public:
  using size_type = uint32_t;

  Message() : data(HEADER_SIZE, 0) {
    size() = 0;
    deadbeef() = DEADBEEF;
  }

  size_type &size() { return *reinterpret_cast<size_type *>(&data[0]); }

  uint32_t &deadbeef() {
    return *reinterpret_cast<uint32_t *>(&data[sizeof(size_type)]);
  }

  void clear() {
    data = std::string(HEADER_SIZE, 0);
    size() = 0;
    deadbeef() = DEADBEEF;
  }

  StringPiece toStringPiece() {
    return StringPiece(data.data() + HEADER_SIZE, data.size() - HEADER_SIZE);
  }

public:
  std::string data;

public:
  static constexpr int HEADER_SIZE = sizeof(size_type) + sizeof(uint32_t);
  static constexpr uint32_t DEADBEEF = 0xDEADBEEF;
};
} // namespace scar