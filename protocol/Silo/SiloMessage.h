//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/Table.h"

namespace scar {

enum class SiloMessage {
  SEARCH_REQUEST,
  SEARCH_RESPONSE,
  LOCK_REQUEST,
  LOCK_RESPONSE,
  READ_VALIDATION_REQUEST,
  READ_VALIDATION_RESPONSE,
  ABORT_REQUEST,
  WRITE_REQUEST,
  REPLICATION_REQUEST,
  NFIELDS
};

// TODO: for some type T, the serialization string length != sizeof(T)
template <class Database> class SiloMessageFactory {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using TableType = ITable<MetaDataType>;

  SiloMessageFactory(DatabaseType &db) : db(db) {}

  void new_search_message(Message &message, std::size_t table_id,
                          std::size_t partition_id, const void *key,
                          uint32_t key_offset) {

    /*
     * The structure of a search request: (primary key, read key offset)
     */

    TableType *table = db.find_table(table_id, partition_id);
    auto key_size = table->keyNBytes();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(SiloMessage::SEARCH_REQUEST), message_size,
        table_id, partition_id);

    scar::Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
  }

  void new_lock_message(Message &message, std::size_t table_id,
                        std::size_t partition_id, const void *key,
                        uint32_t key_offset) {

    /*
     * The structure of a lock request: (primary key, write key offset)
     */

    TableType *table = db.find_table(table_id, partition_id);
    auto key_size = table->keyNBytes();

    auto message_size =
        MessagePiece::get_header_size() + key_size + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(SiloMessage::LOCK_REQUEST), message_size,
        table_id, partition_id);

    scar::Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << key_offset;
    message.flush();
  }

  void new_read_validation_message(Message &message, std::size_t table_id,
                                   std::size_t partition_id, const void *key,
                                   uint32_t key_offset, uint64_t tid) {

    /*
     * The structure of a read validation request: (primary key, read key
     * offset, tid)
     */

    TableType *table = db.find_table(table_id, partition_id);
    auto key_size = table->keyNBytes();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        sizeof(key_offset) + sizeof(tid);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(SiloMessage::READ_VALIDATION_REQUEST),
        message_size, table_id, partition_id);

    scar::Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder << tid;
    message.flush();
  }

  void new_abort_message(Message &message, std::size_t table_id,
                         std::size_t partition_id, const void *key) {

    /*
     * The structure of an abort request: (primary key)
     */

    TableType *table = db.find_table(table_id, partition_id);
    auto key_size = table->keyNBytes();

    auto message_size = MessagePiece::get_header_size() + key_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(SiloMessage::ABORT_REQUEST), message_size,
        table_id, partition_id);

    scar::Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    message.flush();
  }

  void new_write_message(Message &message, std::size_t table_id,
                         std::size_t partition_id, const void *key,
                         const void *value, uint64_t commit_tid) {

    /*
     * The structure of a write request: (primary key, value, commit_tid)
     */

    TableType *table = db.find_table(table_id, partition_id);
    auto key_size = table->keyNBytes();
    auto value_size = table->valueNBytes();

    auto message_size = MessagePiece::get_header_size() + key_size + value_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(SiloMessage::WRITE_REQUEST), message_size,
        table_id, partition_id);

    scar::Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder.write_n_bytes(value, value_size);
    encoder << commit_tid;
    message.flush();
  }

  void new_replication_message(Message &message, std::size_t table_id,
                               std::size_t partition_id, const void *key,
                               const void *value, uint64_t commit_tid) {

    /*
     * The structure of a replication request: (primary key, value, commit_tid)
     */

    TableType *table = db.find_table(table_id, partition_id);
    auto key_size = table->keyNBytes();
    auto value_size = table->valueNBytes();

    auto message_size = MessagePiece::get_header_size() + key_size + value_size;
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(SiloMessage::REPLICATION_REQUEST), message_size,
        table_id, partition_id);

    scar::Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    encoder.write_n_bytes(value, value_size);
    encoder << commit_tid;
    message.flush();
  }

private:
  DatabaseType &db;
};

// TODO: for some type T, the serialization string length != sizeof(T)
template <class Database> class SiloMessageHandler {

public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using TableType = ITable<MetaDataType>;

  SiloMessageHandler(DatabaseType &db) : db(db) {}



private:
  DatabaseType &db;
};

} // namespace scar
