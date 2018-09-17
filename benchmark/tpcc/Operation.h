//
// Created by Yi Lu on 9/17/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/StringPiece.h"

namespace scar {
namespace tpcc {
struct Operation {
  int32_t QUERY_TYPE; // 0 new order, 1 payment

  int32_t W_ID;
  float W_YTD;

  int32_t D_W_ID;
  int32_t D_ID;
  float D_YTD;
  int32_t D_NEXT_O_ID;

  int8_t O_OL_CNT;

  int32_t S_W_ID[15];
  int32_t S_I_ID[15];
  int16_t S_QUANTITY[15];
  float S_YTD[15];
  int32_t S_ORDER_CNT[15];
  int32_t S_REMOTE_CNT[15];

  int32_t C_W_ID;
  int32_t C_D_ID;
  int32_t C_ID;

  std::string C_DATA;
  float C_BALANCE;
  float C_YTD_PAYMENT;
  int32_t C_PAYMENT_CNT;

  void serialize(Message &message, uint32_t message_type) {

    auto message_size = MessagePiece::get_header_size() + sizeof(QUERY_TYPE);

    if (QUERY_TYPE == 0) {
      message_size += sizeof(D_W_ID) + sizeof(D_ID) + sizeof(D_NEXT_O_ID);
      message_size += sizeof(O_OL_CNT);
      message_size +=
          O_OL_CNT * (sizeof(S_W_ID[0]) + sizeof(S_I_ID[0]) + sizeof(S_YTD[0]) +
                      sizeof(S_ORDER_CNT[0]) + sizeof(S_REMOTE_CNT[0]));
    } else {
      message_size += sizeof(W_ID) + sizeof(W_YTD);
      message_size += sizeof(D_W_ID) + sizeof(D_ID) + sizeof(D_YTD);
      message_size += sizeof(C_W_ID) + sizeof(C_D_ID) + sizeof(C_ID);
      message_size += sizeof(uint32_t) + C_DATA.size();
      message_size +=
          sizeof(C_BALANCE) + sizeof(C_YTD_PAYMENT) + sizeof(C_PAYMENT_CNT);
    }

    auto message_piece_header = MessagePiece::construct_message_piece_header(
        message_type, message_size, 0, 0);

    Encoder encoder(message.data);
    encoder << message_piece_header;
    encoder << QUERY_TYPE;
    if (QUERY_TYPE == 0) {
      encoder << D_W_ID << D_ID << D_NEXT_O_ID;
      encoder << O_OL_CNT;
      for (int i = 0; i < O_OL_CNT; i++) {
        encoder << S_W_ID[i] << S_I_ID[i] << S_QUANTITY[i] << S_YTD[i]
                << S_ORDER_CNT[i] << S_REMOTE_CNT[i];
      }
    } else {
      encoder << W_ID << W_YTD;
      encoder << D_W_ID << D_ID << D_YTD;
      encoder << C_W_ID << C_D_ID << C_ID;
      encoder << uint32_t(C_DATA.size());
      encoder.write_n_bytes(C_DATA.data(), C_DATA.size());
      encoder << C_BALANCE << C_YTD_PAYMENT << C_PAYMENT_CNT;
    }
    message.flush();
  }

  void deserialize(StringPiece stringPiece) {
    Decoder dec(stringPiece);
    dec >> QUERY_TYPE;
    if (QUERY_TYPE == 0) {
      dec >> D_W_ID >> D_ID >> D_NEXT_O_ID;
      dec >> O_OL_CNT;
      for (int i = 0; i < O_OL_CNT; i++) {
        dec >> S_W_ID[i] >> S_I_ID[i] >> S_QUANTITY[i] >> S_YTD[i] >>
            S_ORDER_CNT[i] >> S_REMOTE_CNT[i];
      }
    } else {
      dec >> W_ID >> W_YTD;
      dec >> D_W_ID >> D_ID >> D_YTD;
      dec >> C_W_ID >> C_D_ID >> C_ID;
      uint32_t length;
      dec >> length;
      C_DATA.resize(length);
      dec.read_n_bytes(&C_DATA[0], length);
      dec >> C_BALANCE >> C_YTD_PAYMENT >> C_PAYMENT_CNT;
    }
  }
};
} // namespace tpcc
} // namespace scar