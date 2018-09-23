//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "common/Operation.h"

namespace scar {

enum class ControlMessage {
  STATISTICS,
  SIGNAL,
  ACK,
  OPERATION_REPLICATION_REQUEST,
  OPERATION_REPLICATION_RESPONSE,
  NFIELDS
};

class ControlMessageFactory {

public:
  static std::size_t new_statistics_message(Message &message, double value) {
    /*
     * The structure of a statistics message: (statistics value : double)
     *
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(double);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::STATISTICS), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_signal_message(Message &message, uint32_t value) {

    /*
     * The structure of a signal message: (signal value : uint32_t)
     */

    // the message is not associated with a table or a partition, use 0.
    auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::SIGNAL), message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << value;
    message.flush();
    return message_size;
  }

  static std::size_t new_ack_message(Message &message) {
    /*
     * The structure of an ack message: ()
     */

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::ACK), message_size, 0, 0);
    Encoder encoder(message.data);
    encoder << message_piece_header;
    message.flush();
    return message_size;
  }

  static std::size_t
  new_operation_replication_message(Message &message,
                                    const Operation &operation) {

    /*
     * The structure of an operation replication message: (bitvec, data)
     */

    auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) +
                        operation.data.size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ControlMessage::OPERATION_REPLICATION_REQUEST),
        message_size, operation.get_table_id(), operation.get_partition_id());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << operation.bitvec;
    encoder.write_n_bytes(operation.data.c_str(), operation.data.size());
    message.flush();
    return message_size;
  }
};

class ControlMessageHandler {

public:
  template <class DatabaseType>
  static void operation_replication_request_handler(MessagePiece inputPiece,
                                                    Message &responseMessage,
                                                    DatabaseType &db,
                                                    bool require_response) {

    DCHECK(
        inputPiece.get_message_type() ==
        static_cast<uint32_t>(ControlMessage::OPERATION_REPLICATION_REQUEST));

    auto message_size = inputPiece.get_message_length();
    Decoder dec(inputPiece.toStringPiece());
    Operation operation;
    dec >> operation.bitvec;

    auto data_size =
        message_size - MessagePiece::get_header_size() - sizeof(uint32_t);
    DCHECK(data_size > 0);

    operation.data.resize(data_size);
    dec.read_n_bytes(&operation.data[0], data_size);

    DCHECK(dec.size() == 0);

    db.apply_operation(operation);

    if (require_response) {
      // prepare response message header
      auto message_size = MessagePiece::get_header_size();
      auto message_piece_header = MessagePiece::construct_message_piece_header(
          static_cast<uint32_t>(ControlMessage::OPERATION_REPLICATION_RESPONSE),
          message_size, operation.get_table_id(), operation.get_partition_id());

      scar::Encoder encoder(responseMessage.data);
      encoder << message_piece_header;
      responseMessage.flush();
    }
  }
};

} // namespace scar
