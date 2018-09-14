//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

namespace scar {

enum class CalvinMessage {
  NFIELDS = static_cast<int>(ControlMessage::NFIELDS)
};

class CalvinMessageFactory {
  using Table = ITable<std::atomic<uint64_t>>;

public:
};

class CalvinMessageHandler {
  using Table = ITable<std::atomic<uint64_t>>;

public:
  static std::vector<std::function<void(MessagePiece, Message &, Table &)>>
  get_message_handlers() {
    std::vector<std::function<void(MessagePiece, Message &, Table &)>> v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    return v;
  }
};

} // namespace scar