//
// Created by Yi Lu on 9/17/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/StringPiece.h"

namespace scar {
namespace ycsb {
struct Operation {

  void serialize(Message &message, uint32_t message_type) {
    CHECK(false); // not supported
  }

  void deserialize(StringPiece stringPiece) {
    CHECK(false); // not supported
  }
};
} // namespace ycsb
} // namespace scar