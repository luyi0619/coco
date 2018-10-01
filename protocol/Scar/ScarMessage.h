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
  ASYNC_REPLICATION_REQUEST, // for read set replication
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
                                                 uint64_t tid,
                                                 uint64_t commit_ts) {

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

  static std::size_t
  new_async_replication_message(Message &message, Table &table, const void *key,
                                const void *value, uint64_t commit_ts) {

    /*
     * The structure of an async replication request: (primary key, field value,
     * commit_ts)
     */

    auto key_size = table.key_size();
    auto field_size = table.field_size();

    auto message_size = MessagePiece::get_header_size() + key_size +
                        field_size + sizeof(commit_ts);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ScarMessage::ASYNC_REPLICATION_REQUEST),
        message_size, table.tableID(), table.partitionID());

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
        static_cast<uint32_t>(ScarMessage::RTS_REPLICATION_REQUEST),
        message_size, table.tableID(), table.partitionID());

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
                                     Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::SEARCH_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read request: (primary key, read key offset)
     * The structure of a read response: (value, tid, read key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    // get row and offset
    const void *key = stringPiece.data();
    auto row = table.search(key);

    stringPiece.remove_prefix(key_size);
    scar::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + value_size +
                        sizeof(uint64_t) + sizeof(key_offset);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ScarMessage::SEARCH_RESPONSE), message_size,
        table_id, partition_id);

    scar::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;

    // reserve size for read
    responseMessage.data.append(value_size, 0);
    void *dest =
        &responseMessage.data[0] + responseMessage.data.size() - value_size;
    // read to message buffer
    auto tid = ScarHelper::read(row, dest, value_size);
    encoder << tid << key_offset;
    responseMessage.flush();
  }

  static void search_response_handler(MessagePiece inputPiece,
                                      Message &responseMessage, Table &table,
                                      Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::SEARCH_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read response: (value, tid, read key offset)
     */

    uint64_t tid;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  value_size + sizeof(tid) +
                                                  sizeof(key_offset));

    StringPiece stringPiece = inputPiece.toStringPiece();
    stringPiece.remove_prefix(value_size);
    Decoder dec(stringPiece);
    dec >> tid >> key_offset;

    ScarRWKey &readKey = txn.readSet[key_offset];
    dec = Decoder(inputPiece.toStringPiece());
    dec.read_n_bytes(readKey.get_value(), value_size);
    readKey.set_tid(tid);
    txn.pendingResponses--;
    txn.network_size += inputPiece.get_message_length();
  }

  static void lock_request_handler(MessagePiece inputPiece,
                                   Message &responseMessage, Table &table,
                                   Transaction &txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::LOCK_REQUEST));

    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a lock request: (primary key, write key offset)
     * The structure of a lock response: (success?, tid, write key offset)
     */

    auto stringPiece = inputPiece.toStringPiece();

    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset));

    const void *key = stringPiece.data();
    std::atomic<uint64_t> &tid = table.search_metadata(key);

    bool success;
    uint64_t latest_tid = ScarHelper::lock(tid, success);

    stringPiece.remove_prefix(key_size);
    scar::Decoder dec(stringPiece);
    dec >> key_offset;

    DCHECK(dec.size() == 0);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool) +
                        sizeof(uint64_t) + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ScarMessage::LOCK_RESPONSE), message_size,
        table_id, partition_id);

    scar::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << latest_tid << key_offset;
    responseMessage.flush();
  }

  static void lock_response_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Table &table,
                                    Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a lock response: (success?, tid, write key offset)
     */

    bool success;
    uint64_t latest_tid;
    uint32_t key_offset;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + sizeof(success) +
               sizeof(latest_tid) + sizeof(key_offset));

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> latest_tid >> key_offset;

    DCHECK(dec.size() == 0);

    ScarRWKey &writeKey = txn.writeSet[key_offset];

    if (success) {

      ScarRWKey *readKey = txn.get_read_key(writeKey.get_key());

      DCHECK(readKey != nullptr);

      uint64_t tid_on_read = readKey->get_tid();

      if (ScarHelper::get_wts(latest_tid) != ScarHelper::get_wts(tid_on_read)) {
        txn.abort_lock = true;
      }

      writeKey.set_tid(latest_tid);
      writeKey.set_write_lock_bit();
    } else {
      txn.abort_lock = true;
    }

    txn.pendingResponses--;
    txn.network_size += inputPiece.get_message_length();
  }

  static void read_validation_request_handler(MessagePiece inputPiece,
                                              Message &responseMessage,
                                              Table &table, Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::READ_VALIDATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a read validation request: (primary key, key offset,
     *                                              tid, commit_ts)
     * The structure of a read validation response: (success?, written_ts,
     *                                               key offset)
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint32_t) +
               sizeof(uint64_t) + sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();
    const void *key = stringPiece.data();
    std::atomic<uint64_t> &latest_tid = table.search_metadata(key);
    stringPiece.remove_prefix(key_size);

    uint32_t key_offset;
    uint64_t tid, commit_ts;
    Decoder dec(stringPiece);
    dec >> key_offset >> tid >> commit_ts;

    uint64_t written_ts = tid;
    DCHECK(ScarHelper::is_locked(written_ts) == false);

    bool success =
        ScarHelper::validate_read_key(latest_tid, tid, commit_ts, written_ts);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool) +
                        sizeof(uint64_t) + sizeof(uint32_t);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ScarMessage::READ_VALIDATION_RESPONSE),
        message_size, table_id, partition_id);

    scar::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << written_ts << key_offset;

    responseMessage.flush();
  }

  static void read_validation_response_handler(MessagePiece inputPiece,
                                               Message &responseMessage,
                                               Table &table, Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::READ_VALIDATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a read validation response: (success?, written_ts,
     *                                               key offset)
     */

    bool success;
    uint64_t written_ts;
    uint32_t key_offset;

    Decoder dec(inputPiece.toStringPiece());

    dec >> success >> written_ts >> key_offset;

    ScarRWKey &readKey = txn.readSet[key_offset];

    if (success) {
      readKey.set_read_validation_success_bit();
      if (ScarHelper::get_wts(written_ts) !=
          ScarHelper::get_wts(readKey.get_tid())) {
        DCHECK(ScarHelper::get_wts(written_ts) >
               ScarHelper::get_wts(readKey.get_tid()));
        readKey.set_wts_change_in_read_validation_bit();
        readKey.set_tid(written_ts);
      }
    }

    txn.pendingResponses--;
    txn.network_size += inputPiece.get_message_length();

    if (!success) {
      txn.abort_read_validation = true;
    }
  }

  static void abort_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Table &table,
                                    Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::ABORT_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of an abort request: (primary key)
     * The structure of an abort response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size);

    auto stringPiece = inputPiece.toStringPiece();
    const void *key = stringPiece.data();
    std::atomic<uint64_t> &tid = table.search_metadata(key);

    // unlock the key
    ScarHelper::unlock(tid);
  }

  static void write_request_handler(MessagePiece inputPiece,
                                    Message &responseMessage, Table &table,
                                    Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::WRITE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a write request: (primary key, field value)
     * The structure of a write response: ()
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + field_size);

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    table.deserialize_value(key, stringPiece);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ScarMessage::WRITE_RESPONSE), message_size,
        table_id, partition_id);

    scar::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void write_response_handler(MessagePiece inputPiece,
                                     Message &responseMessage, Table &table,
                                     Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::WRITE_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());

    /*
     * The structure of a write response: ()
     */

    txn.pendingResponses--;
    txn.network_size += inputPiece.get_message_length();
  }

  static void replication_request_handler(MessagePiece inputPiece,
                                          Message &responseMessage,
                                          Table &table, Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a replication request: (primary key, field value,
     * commit_ts).
     * The structure of a replication response: ()
     */

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    uint64_t commit_ts;
    Decoder dec(stringPiece);
    dec >> commit_ts;

    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    uint64_t last_tid = ScarHelper::lock(tid);
    DCHECK(ScarHelper::get_wts(last_tid) < ScarHelper::get_wts(commit_ts));
    table.deserialize_value(key, valueStringPiece);
    ScarHelper::unlock(tid, commit_ts);

    // prepare response message header
    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(ScarMessage::REPLICATION_RESPONSE), message_size,
        table_id, partition_id);

    scar::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.flush();
  }

  static void replication_response_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           Table &table, Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::REPLICATION_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a replication response: ()
     */

    txn.pendingResponses--;
    txn.network_size += inputPiece.get_message_length();
  }

  static void async_replication_request_handler(MessagePiece inputPiece,
                                                Message &responseMessage,
                                                Table &table,
                                                Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::ASYNC_REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of an async replication request: (primary key, field value,
     * commit_ts).
     * The structure of a replication response: null
     */

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    uint64_t commit_ts;
    Decoder dec(stringPiece);
    dec >> commit_ts;

    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    uint64_t last_tid = ScarHelper::lock(tid);
    DCHECK(ScarHelper::get_wts(last_tid) < ScarHelper::get_wts(commit_ts));
    table.deserialize_value(key, valueStringPiece);
    ScarHelper::unlock(tid, commit_ts);
  }

  static void rts_replication_request_handler(MessagePiece inputPiece,
                                              Message &responseMessage,
                                              Table &table, Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::RTS_REPLICATION_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a release lock request: (primary key, commit_ts)
     * The structure of a write response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    uint64_t commit_ts;
    Decoder dec(stringPiece);
    dec >> commit_ts;
    DCHECK(dec.size() == 0);

    DCHECK(ScarHelper::get_wts(commit_ts) <= ScarHelper::get_rts(commit_ts));

    // update the record's rts, if the wts is the same and the rts in
    // replication is larger

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    uint64_t last_tid = ScarHelper::lock(tid);

    if (ScarHelper::get_wts(commit_ts) == ScarHelper::get_wts(last_tid) &&
        ScarHelper::get_rts(commit_ts) > ScarHelper::get_rts(last_tid)) {
      ScarHelper::unlock(tid, commit_ts);
    } else {
      ScarHelper::unlock(tid);
    }
  }

  static void release_lock_request_handler(MessagePiece inputPiece,
                                           Message &responseMessage,
                                           Table &table, Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(ScarMessage::RELEASE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();

    /*
     * The structure of a release lock request: (primary key, commit_ts)
     * The structure of a write response: null
     */

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);

    uint64_t commit_ts;
    Decoder dec(stringPiece);
    dec >> commit_ts;
    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);
    ScarHelper::unlock(tid, commit_ts);
  }

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
    v.push_back(async_replication_request_handler);
    v.push_back(rts_replication_request_handler);
    v.push_back(release_lock_request_handler);
    return v;
  }
};

} // namespace scar