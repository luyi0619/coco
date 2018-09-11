//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include "protocol/TwoPL/TwoPLTransaction.h"

namespace scar {

enum class TwoPLMessage {
  READ_LOCK_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  READ_LOCK_RESPONSE,
  WRITE_LOCK_REQUEST,
  WRITE_LOCK_RESPONSE,
  WRITE_REQUEST,
  WRITE_RESPONSE,
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  RELEASE_READ_LOCK_REQUEST,
  RELEASE_READ_LOCK_RESPONSE,
  RELEASE_WRITE_LOCK_REQUEST,
  RELEASE_WRITE_LOCK_RESPONSE,
  NFIELDS
};

class TwoPLMessageFactory {

  using Table = ITable<std::atomic<uint64_t>>;

public:
  static void new_read_lock_message(Message &message, Table &table,
                                    const void *key, uint32_t key_offset) {

    /*
     * The structure of a read lock request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(TwoPLMessage::READ_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
  }

  static void new_write_lock_message(Message &message, Table &table,
                                     const void *key, uint32_t key_offset) {

    /*
     * The structure of a write lock request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(TwoPLMessage::WRITE_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
  }

  static void new_write_message(Message &message, Table &table, const void *key,
                                const void *value) {

    /*
     * The structure of a write request: (primary key, field value)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + field_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(TwoPLMessage::WRITE_LOCK_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    message.flush();
  }

  static void new_replication_message(Message &message, Table &table,
                                      const void *key, const void *value,
                                      uint64_t commit_tid) {

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_tid)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(TwoPLMessage::REPLICATION_REQUEST), message_size,
        table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
  }

  static void new_release_read_lock_message(Message &message, Table &table,
                                            const void *key,
                                            const void *value) {
    /*
     * The structure of a release read lock request: (primary key)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(TwoPLMessage::RELEASE_READ_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    message.flush();
  }

  static void new_release_write_lock_message(Message &message, Table &table,
                                             const void *key, const void *value,
                                             uint64_t commit_tid) {

    /*
     * The structure of a replication request: (primary key, commit tid)
     */

    auto key_size = table.key_size();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(TwoPLMessage::RELEASE_WRITE_LOCK_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << commit_tid;
    message.flush();
  }
};

template <class Database> class TwoPLMessageHandler {
  using Table = ITable<std::atomic<uint64_t>>;
  using Transaction = SiloTransaction<Database>;

public:
  static void read_lock_request_handler(MessagePiece inputPiece,
                                        Message &responseMessage, Table &table,
                                        Transaction &txn) {}

  static void read_lock_response_handler(MessagePiece inputPiece,
                                         Message &responseMessage, Table &table,
                                         Transaction &txn) {}
  static void write_lock_request_handler(MessagePiece inputPiece,
                                         Message &responseMessage, Table &table,
                                         Transaction &txn) {}

  static void write_lock_response_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Table &table, Transaction &txn) {}

  static void write_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Table &table,
                                    Transaction &txn) {}

  static void write_response_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Table &table,
                                     Transaction &txn) {}

  static void replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Table &table, Transaction &txn) {}

  static void replication_response_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           Table &table, Transaction &txn) {}

  static void release_read_lock_request_handler(MessagePiece inputPiece,
                                                Message &responseMessage,
                                                Table &table,
                                                Transaction &txn) {}

  static void release_read_lock_response_handler(MessagePiece inputPiece,
                                                 Message &responseMessage,
                                                 Table &table,
                                                 Transaction &txn) {}

  static void release_write_lock_request_handler(MessagePiece inputPiece,
                                                 Message &responseMessage,
                                                 Table &table,
                                                 Transaction &txn) {}

  static void release_write_lock_response_handler(MessagePiece inputPiece,
                                                  Message &responseMessage,
                                                  Table &table,
                                                  Transaction &txn) {}

public:
  static std::vector<
      std::function<void(MessagePiece, Message &, Table &, Transaction &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Table &, Transaction &)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(read_lock_request_handler);
    v.push_back(read_lock_response_handler);
    v.push_back(write_lock_request_handler);
    v.push_back(write_lock_response_handler);
    v.push_back(write_request_handler);
    v.push_back(write_response_handler);
    v.push_back(replication_request_handler);
    v.push_back(replication_response_handler);
    v.push_back(release_read_lock_request_handler);
    v.push_back(release_read_lock_response_handler);
    v.push_back(release_write_lock_request_handler);
    v.push_back(release_write_lock_response_handler);
    return v;
  }
};

} // namespace scar
