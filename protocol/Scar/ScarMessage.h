//
// Created by Yi Lu on 9/19/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/Scar/ScarHelper.h"
#include "protocol/Scar/ScarRWKey.h"
#include "protocol/Scar/ScarTransaction.h"

namespace scar {

enum class ScarMessage {
  SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  SEARCH_RESPONSE,
  LOCK_REQUEST,
  LOCK_RESPONSE,
  READ_VALIDATION_REQUEST,
  READ_VALIDATION_RESPONSE,
  ABORT_REQUEST,
  WRITE_REQUEST,
  WRITE_RESPONSE,
  REPLICATION_REQUEST,
  REPLICATION_RESPONSE,
  RTS_REPLICATION_REQUEST,
  RELEASE_LOCK_REQUEST,
  NFIELDS
};

class ScarMessageFactory {
  using Table = ITable<std::atomic<uint64_t>>;
public:

  static std::size_t new_search_message(Message &message, Table &table,
                                        const void *key, uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
            MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::SEARCH_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_lock_message(Message &message, Table &table,
                                      const void *key, uint32_t key_offset) {

    /*
     * The structure of a lock request: (primary key, write key offset)
     */

    auto key_size = table.key_size();

    auto message_size =
            MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::LOCK_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
    return message_size;
  }

  static std::size_t new_read_validation_message(Message &message, Table &table,
                                                 const void *key,
                                                 uint32_t key_offset,
                                                 uint64_t tid, uint64_t commit_ts) {

    /*
     * The structure of a read validation request: (primary key, read key
     * offset, tid, commit_ts)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(key_offset) + sizeof(tid) + sizeof(commit_ts);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::READ_VALIDATION_REQUEST),
            message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset << tid << commit_ts;
    message.flush();
    return message_size;
  }

  static std::size_t new_abort_message(Message &message, Table &table,
                                       const void *key) {

    /*
     * The structure of an abort request: (primary key)
     */

    auto key_size = table.key_size();

    auto message_size = MessagePiece::get_header_size() + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::ABORT_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    message.flush();
    return message_size;
  }

  static std::size_t new_write_message(Message &message, Table &table,
                                       const void *key, const void *value) {

    /*
     * The structure of a write request: (primary key, field value)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + field_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::WRITE_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    message.flush();
    return message_size;
  }

  static std::size_t new_replication_message(Message &message, Table &table,
                                             const void *key, const void *value,
                                             uint64_t commit_ts) {

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_ts)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_ts);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_ts;
    message.flush();
    return message_size;
  }


  static std::size_t new_rts_replication_message(Message &message, Table &table,
                                             const void *key, uint64_t ts) {

    /*
     * The structure of a rts replication request: (primary key, ts)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size + sizeof(ts);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::RTS_REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << ts;
    message.flush();
    return message_size;
  }


  static std::size_t new_release_lock_message(Message &message, Table &table,
                                              const void *key,
                                              uint64_t commit_ts) {
    /*
     * The structure of a replication request: (primary key, commit ts)
     */

    auto key_size = table.key_size();

    auto message_size =
            MessagePiece::get_header_size() + key_size + sizeof(commit_ts);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(ScarMessage::RELEASE_LOCK_REQUEST), message_size,
            table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << commit_ts;
    message.flush();
    return message_size;
  }


};

class ScarMessageHandler {
  using Table = ITable<std::atomic<uint64_t>>;
  using Transaction = ScarTransaction;

public:
  static void search_request_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Table &table,
                                     Transaction &txn) {}

  static void search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Table &table,
                                      Transaction &txn) {}

  static void lock_request_handler(MessagePiece inputPiece,
                                   Message &responseMessage, Table &table,
                                   Transaction &txn) {}

  static void lock_response_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Table &table,
                                    Transaction &txn) {}

  static void read_validation_request_handler(MessagePiece inputPiece,
                                              Message &responseMessage,
                                              Table &table, Transaction &txn) {}

  static void read_validation_response_handler(MessagePiece inputPiece,
                                               Message &responseMessage,
                                               Table &table, Transaction &txn) {

  }

  static void abort_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Table &table,
                                    Transaction &txn) {}

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

  static void rts_replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Table &table, Transaction &txn) {}


  static void release_lock_request_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           Table &table, Transaction &txn) {}

  static std::vector<
      std::function<void(MessagePiece, Message &, Table &, Transaction &)>>
  get_message_handlers() {
    std::vector<
        std::function<void(MessagePiece, Message &, Table &, Transaction &)>>
        v;
    v.resize(static_cast<int>(ControlMessage::NFIELDS));
    v.push_back(search_request_handler);
    v.push_back(search_response_handler);
    v.push_back(lock_request_handler);
    v.push_back(lock_response_handler);
    v.push_back(read_validation_request_handler);
    v.push_back(read_validation_response_handler);
    v.push_back(abort_request_handler);
    v.push_back(write_request_handler);
    v.push_back(write_response_handler);
    v.push_back(replication_request_handler);
    v.push_back(replication_response_handler);
    v.push_back(rts_replication_request_handler);
    v.push_back(release_lock_request_handler);
    return v;
  }
};

} // namespace scar