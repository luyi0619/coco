//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/RStore/RStoreHelper.h"
#include "protocol/RStore/RStoreRWKey.h"

namespace scar {

enum class RStoreMessage {
  REPLICATION_VALUE_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
  REPLICATION_OPERATION_REQUEST,
  SIGNAL,
  C_PHASE_ACK,
  S_PHASE_ACK,
  NFIELDS
};

template <class Table> class RStoreMessageFactory {
public:
  static void new_replication_value_message(Message &message, Table &table,
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
        static_cast<uint32_t>(RStoreMessage::REPLICATION_VALUE_REQUEST),
        message_size, table.tableID(), table.partitionID());

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder.write_n_bytes(key, key_size);
    table.serialize_value(encoder, value);
    encoder << commit_tid;
    message.flush();
  }

  static void new_replication_operation_message(Message &message) {}

  static void new_signal_message(Message &message) {}

  static void new_c_phase_ack_message(Message &message) {}

  static void new_s_phase_ack_message(Message &message) {}
};

template <class Table, class Transaction> class RStoreMessageHandler {
public:
  static void replication_value_request_handler(MessagePiece inputPiece,
                                                Message &responseMessage,
                                                Table &table,
                                                Transaction &txn) {

    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(RStoreMessage::REPLICATION_VALUE_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    /*
     * The structure of a replication value request:
     *      (primary key, field value, commit_tid).
     * The structure of a replication value response: null
     */

    DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                  key_size + field_size +
                                                  sizeof(uint64_t));

    auto stringPiece = inputPiece.toStringPiece();

    const void *key = stringPiece.data();
    stringPiece.remove_prefix(key_size);
    auto valueStringPiece = stringPiece;
    stringPiece.remove_prefix(field_size);

    uint64_t commit_tid;
    Decoder dec(stringPiece);
    dec >> commit_tid;

    DCHECK(dec.size() == 0);

    std::atomic<uint64_t> &tid = table.search_metadata(key);

    uint64_t last_tid = RStoreHelper::lock(tid);

    if (commit_tid > last_tid) {
      table.deserialize_value(key, valueStringPiece);
      RStoreHelper::unlock(tid, commit_tid);
    } else {
      RStoreHelper::unlock(tid);
    }
  }
};
} // namespace scar