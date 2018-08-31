//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/tpcc/Query.h"
#include "common/Time.h"
#include "core/Transaction.h"

namespace scar {
namespace tpcc {

template <class Protocol> class NewOrder : public Transaction<Protocol> {
public:
  using ProtocolType = Protocol;
  using RWKeyType = typename Protocol::RWKeyType;
  using DatabaseType = typename Protocol::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;

  static_assert(
      std::is_same<MetaDataType, typename Protocol::MetaDataType>::value,
      "The database datatype is different from the one in protocol.");

  NewOrder(DatabaseType &db, ContextType &context, RandomType &random,
           ProtocolType &protocol)
      : Transaction<ProtocolType>(db, context, random, protocol) {}

  virtual ~NewOrder() override = default;

  TransactionResult execute() override {

    ContextType &context = this->context;
    RandomType &random = this->random;

    int32_t W_ID = random.uniform_dist(1, context.partitionNum);
    NewOrderQuery query = makeNewOrderQuery()(context, W_ID, random);

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
    // the warehouse tax rate, is retrieved.

    auto warehouseTableID = warehouse::tableID;
    warehouse::key warehouse_key(W_ID);
    warehouse::value warehouse_value;
    this->search(warehouseTableID, W_ID - 1, warehouse_key, warehouse_value);

    float W_TAX = warehouse_value.W_YTD;

    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    // available order number for the district, is retrieved and incremented by
    // one.

    auto districtTableID = district::tableID;
    district::key district_key(W_ID, D_ID);
    district::value district_value;
    this->search(districtTableID, W_ID - 1, district_key, district_value);

    float D_TAX = district_value.D_TAX;
    int32_t D_NEXT_O_ID = district_value.D_NEXT_O_ID;

    district_value.D_NEXT_O_ID += 1;
    this->update(districtTableID, W_ID - 1, district_key, district_value);

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = customer::tableID;
    customer::key customer_key(W_ID, D_ID, C_ID);
    customer::value customer_value;
    this->search(customerTableID, W_ID - 1, customer_key, customer_value);

    float C_DISCOUNT = customer_value.C_DISCOUNT;

    // A new row is inserted into both the NEW-ORDER table and the ORDER table
    // to reflect the creation of the new order. O_CARRIER_ID is set to a null
    // value. If the order includes only home order-lines, then O_ALL_LOCAL is
    // set to 1, otherwise O_ALL_LOCAL is set to 0.

    new_order::key new_order_key(W_ID, D_ID, D_NEXT_O_ID);

    order::key order_key(W_ID, D_ID, D_NEXT_O_ID);
    order::value order_value;

    order_value.O_ENTRY_D = Time::now();
    order_value.O_CARRIER_ID = 0;
    order_value.O_OL_CNT = query.O_OL_CNT;
    order_value.O_C_ID = query.C_ID;
    order_value.O_ALL_LOCAL = !query.isRemote();

    float total_amount = 0;

    auto itemTableID = item::tableID;
    item::key item_keys[15];
    item::value item_values[15];

    auto stockTableID = stock::tableID;
    stock::key stock_keys[15];
    stock::value stock_values[15];

    auto orderLineTableID = stock::tableID;
    order_line::key order_line_keys[15];
    order_line::value order_line_values[15];

    for (int i = 0; i < query.O_OL_CNT; i++) {

      // The row in the ITEM table with matching I_ID (equals OL_I_ID) is
      // selected and I_PRICE, the price of the item, I_NAME, the name of the
      // item, and I_DATA are retrieved. If I_ID has an unused value (see
      // Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
      // rollback of the database transaction (see Clause 2.4.2.3).

      int32_t OL_I_ID = query.INFO[i].OL_I_ID;
      int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

      item_keys[i] = item::key(OL_I_ID);

      // If I_ID has an unused value, rollback.
      // In OCC, rollback can return without going through commit protocal

      if (item_keys[i].I_ID == 0) {
        // abort();
        return TransactionResult::ABORT_NORETRY;
      }

      this->search(itemTableID, 0, item_keys[i], item_values[i]);
      float I_PRICE = item_values[i].I_PRICE;

      // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
      // S_W_ID (equals OL_SUPPLY_W_ID) is selected.

      stock_keys[i] = stock::key(OL_SUPPLY_W_ID, OL_I_ID);

      this->search(stockTableID, OL_SUPPLY_W_ID - 1, stock_keys[i],
                   stock_values[i]);

      // S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the
      // district number, and S_DATA are retrieved. If the retrieved value for
      // S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is
      // decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
      // (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and
      // S_ORDER_CNT is incremented by 1. If the order-line is remote, then
      // S_REMOTE_CNT is incremented by 1.

      if (stock_values[i].S_QUANTITY >= OL_QUANTITY + 10) {
        stock_values[i].S_QUANTITY -= OL_QUANTITY;
      } else {
        stock_values[i].S_QUANTITY =
            stock_values[i].S_QUANTITY - OL_QUANTITY + 91;
      }

      stock_values[i].S_YTD += OL_QUANTITY;
      stock_values[i].S_ORDER_CNT++;

      if (OL_SUPPLY_W_ID != W_ID) {
        stock_values[i].S_REMOTE_CNT++;
      }

      this->update(stockTableID, OL_SUPPLY_W_ID - 1, stock_keys[i],
                   stock_values[i]);

      float OL_AMOUNT = I_PRICE * OL_QUANTITY;
      order_line_keys[i] = order_line::key(W_ID, D_ID, D_NEXT_O_ID, i + 1);

      order_line_values[i].OL_I_ID = OL_I_ID;
      order_line_values[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
      order_line_values[i].OL_DELIVERY_D = 0;
      order_line_values[i].OL_QUANTITY = OL_QUANTITY;
      order_line_values[i].OL_AMOUNT = OL_AMOUNT;

      switch (D_ID) {
      case 1:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_01;
        break;
      case 2:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_02;
        break;
      case 3:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_03;
        break;
      case 4:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_04;
        break;
      case 5:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_05;
        break;
      case 6:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_06;
        break;
      case 7:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_07;
        break;
      case 8:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_08;
        break;
      case 9:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_09;
        break;
      case 10:
        order_line_values[i].OL_DIST_INFO = stock_values[i].S_DIST_10;
        break;
      default:
        CHECK(false);
        break;
      }
      total_amount += OL_AMOUNT * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
    }

    return this->commit();
  }
};

template <class Protocol> class Payment : public Transaction<Protocol> {
public:
  using ProtocolType = Protocol;
  using RWKeyType = typename Protocol::RWKeyType;
  using DatabaseType = typename Protocol::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;

  static_assert(
      std::is_same<MetaDataType, typename Protocol::MetaDataType>::value,
      "The database datatype is different from the one in protocol.");

  Payment(DatabaseType &db, ContextType &context, RandomType &random,
          ProtocolType &protocol)
      : Transaction<ProtocolType>(db, context, random, protocol) {}

  virtual ~Payment() override = default;

  TransactionResult execute() override {

    ContextType &context = this->context;
    RandomType &random = this->random;

    int32_t W_ID = random.uniform_dist(1, context.partitionNum);
    PaymentQuery query = makePaymentQuery()(context, W_ID, random);

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    // The row in the WAREHOUSE table with matching W_ID is selected.
    // W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
    // and W_YTD,

    auto warehouseTableID = warehouse::tableID;
    warehouse::key warehouse_key(W_ID);
    warehouse::value warehouse_value;
    this->search(warehouseTableID, W_ID - 1, warehouse_key, warehouse_value);

    // the warehouse's year-to-date balance, is increased by H_ AMOUNT.
    warehouse_value.W_YTD += H_AMOUNT;
    this->update(warehouseTableID, W_ID - 1, warehouse_key, warehouse_value);

    // The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
    // D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
    // and D_YTD,

    auto districtTableID = district::tableID;
    district::key district_key(W_ID, D_ID);
    district::value district_value;
    this->search(districtTableID, W_ID - 1, district_key, district_value);

    // the district's year-to-date balance, is increased by H_AMOUNT.
    district_value.D_YTD += H_AMOUNT;
    this->update(districtTableID, W_ID - 1, district_key, district_value);

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerNameIdxTableID = customer_name_idx::tableID;
    customer_name_idx::key customer_name_idx_key;
    customer_name_idx::value customer_name_idx_value;

    if (C_ID == 0) {
      customer_name_idx_key =
          customer_name_idx::key(C_W_ID, C_D_ID, query.C_LAST);
      this->search(customerNameIdxTableID, C_W_ID - 1, customer_name_idx_key,
                   customer_name_idx_value);
      C_ID = customer_name_idx_value.C_ID;
    }

    auto customerTableID = customer::tableID;
    customer::key customer_key(C_W_ID, C_D_ID, C_ID);
    customer::value customer_value;
    this->search(customerTableID, C_W_ID - 1, customer_key, customer_value);

    if (customer_value.C_CREDIT == "BC") {

      char C_DATA[501];
      int written, total_written = 0;

      written = std::sprintf(C_DATA + total_written, "%d ", C_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", C_D_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", C_W_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", D_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%d ", W_ID);
      total_written += written;

      written = std::sprintf(C_DATA + total_written, "%.2f ", H_AMOUNT);
      total_written += written;

      const char *old_C_DATA = customer_value.C_DATA.c_str();

      int k = total_written;

      for (std::size_t i = 0; i < customer_value.C_DATA.length() && k < 500;
           i++, k++) {
        C_DATA[k] = old_C_DATA[i];
      }
      C_DATA[k] = 0;

      customer_value.C_DATA.assign(C_DATA);
    }

    customer_value.C_BALANCE -= H_AMOUNT;
    customer_value.C_YTD_PAYMENT += H_AMOUNT;
    customer_value.C_PAYMENT_CNT += 1;

    this->update(customerTableID, C_W_ID - 1, customer_key, customer_value);

    char H_DATA[25];
    int written;

    written = std::sprintf(H_DATA, "%s    %s", warehouse_value.W_NAME.c_str(),
                           district_value.D_NAME.c_str());
    H_DATA[written] = 0;

    history::key h_key(W_ID, D_ID, C_W_ID, C_D_ID, C_ID, Time::now());
    history::value h_value;
    h_value.H_AMOUNT = H_AMOUNT;
    h_value.H_DATA.assign(H_DATA, written);

    return this->commit();
  }
};

} // namespace tpcc
} // namespace scar
