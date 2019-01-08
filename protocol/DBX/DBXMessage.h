//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/DBX/DBXRWKey.h"
#include "protocol/DBX/DBXTransaction.h"

namespace scar {

enum class DBXMessage {
  READ_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  NFIELDS
};

class DBXMessageFactory {
  using Table = ITable<std::atomic<uint64_t>>;

public:
  static std::size_t new_read_message(Message &message, Table &table,
                                      uint32_t tid, uint32_t key_offset,
                                      const void *value) {
    return 0;
  }
};

class DBXMessageHandler {
  using Table = ITable<std::atomic<uint64_t>>;
  using Transaction = DBXTransaction;

public:
  static void
  read_request_handler(MessagePiece inputPiece, Message &responseMessage,
                       Table &table,
                       std::vector<std::unique_ptr<Transaction>> &txns) {}

  static std::vector<
      std::function<void(MessagePiece, Message &, Table &,
                         std::vector<std::unique_ptr<Transaction>> &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Table &,
                           std::vector<std::unique_ptr<Transaction>> &)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(read_request_handler);
    return v;
  }
};

} // namespace scar