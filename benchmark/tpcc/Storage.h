//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/tpcc/Schema.h"

namespace scar {
namespace tpcc {
struct Storage {
  warehouse::key warehouse_key;
  warehouse::value warehouse_value;

  district::key district_key;
  district::value district_value;

  customer_name_idx::key customer_name_idx_key;
  customer_name_idx::value customer_name_idx_value;

  customer::key customer_key;
  customer::value customer_value;

  item::key item_keys[15];
  item::value item_values[15];

  stock::key stock_keys[15];
  stock::value stock_values[15];

  new_order::key new_order_key;

  order::key order_key;
  order::value order_value;

  order_line::key order_line_keys[15];
  order_line::value order_line_values[15];

  history::key h_key;
  history::value h_value;
};

struct OperationStorage {
  int32_t QUERY_TYPE; // 0 new order, 1 payment

  int32_t W_ID;
  float W_YTD;

  int32_t D_W_ID;
  int32_t D_ID;
  float D_YTD;
  int32_t D_NEXT_O_ID;

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
};

} // namespace tpcc
} // namespace scar