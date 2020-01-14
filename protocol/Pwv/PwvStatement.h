//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Query.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "protocol/Pwv/PwvHelper.h"
#include "protocol/Pwv/PwvRWKey.h"

namespace scar {

class PwvStatement {
public:
  virtual ~PwvStatement() = default;

  virtual void prepare_read_and_write_set() = 0;

  virtual void execute() = 0;

public:
  std::vector<PwvRWKey> readSet, writeSet;
};

class PwvYCSBStatement : public PwvStatement {
public:
  static constexpr std::size_t keys_num = 10;

  PwvYCSBStatement(ycsb::Database &db, const ycsb::Context &context,
                   ycsb::Random &random, ycsb::Storage &storage,
                   std::size_t partition_id,
                   const ycsb::YCSBQuery<keys_num> &query, int idx)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query), idx(idx) {}

  ~PwvYCSBStatement() override = default;

  void prepare_read_and_write_set() override {
    int ycsbTableID = ycsb::ycsb::tableID;

    auto key = query.Y_KEY[idx];
    storage.ycsb_keys[idx].Y_KEY = key;
    PwvRWKey rwkey;
    rwkey.set_table_id(ycsbTableID);
    rwkey.set_partition_id(context.getPartitionID(key));
    rwkey.set_key(&storage.ycsb_keys[idx]);
    rwkey.set_value(&storage.ycsb_values[idx]);

    readSet.push_back(rwkey);
    if (query.UPDATE[idx]) {
      writeSet.push_back(rwkey);
    }

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(query.UPDATE[idx] ? writeSet.size() == 1 : writeSet.size() == 0)
        << "Check on writeSet size failed!";
  }

  void execute() override {

    int ycsbTableID = ycsb::ycsb::tableID;

    // read
    auto tableId = ycsbTableID;
    auto partitionId = context.getPartitionID(storage.ycsb_keys[idx].Y_KEY);
    auto key = &storage.ycsb_keys[idx];
    auto value = &storage.ycsb_values[idx];
    ITable *table = db.find_table(tableId, partitionId);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    PwvHelper::read(row, value, value_bytes);

    // compute
    if (query.UPDATE[idx]) {
      ycsb::Random local_random;
      storage.ycsb_values[idx].Y_F01.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F02.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F03.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F04.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F05.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F06.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F07.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F08.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F09.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
      storage.ycsb_values[idx].Y_F10.assign(
          local_random.a_string(ycsb::YCSB_FIELD_SIZE, ycsb::YCSB_FIELD_SIZE));
    }

    // write
    if (query.UPDATE[idx]) {
      table->update(key, value);
    }
  }

public:
  ycsb::Database &db;
  const ycsb::Context &context;
  ycsb::Random &random;
  ycsb::Storage &storage;
  std::size_t partition_id;
  const ycsb::YCSBQuery<keys_num> query;
  int idx;
};

class PwvNewOrderWarehouseStatement : public PwvStatement {
public:
  PwvNewOrderWarehouseStatement(tpcc::Database &db,
                                const tpcc::Context &context,
                                tpcc::Random &random, tpcc::Storage &storage,
                                std::size_t partition_id,
                                const tpcc::NewOrderQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvNewOrderWarehouseStatement() override = default;

  void prepare_read_and_write_set() override {

    int32_t W_ID = partition_id + 1;

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;

    // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
    // the warehouse tax rate, is retrieved.

    auto warehouseTableID = tpcc::warehouse::tableID;
    storage.warehouse_key = tpcc::warehouse::key(W_ID);

    PwvRWKey warehouse_rwkey;
    warehouse_rwkey.set_table_id(warehouseTableID);
    warehouse_rwkey.set_partition_id(W_ID - 1);
    warehouse_rwkey.set_key(&storage.warehouse_key);
    warehouse_rwkey.set_value(&storage.warehouse_value);
    readSet.push_back(warehouse_rwkey);

    // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
    // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
    // available order number for the district, is retrieved and incremented by
    // one.

    auto districtTableID = tpcc::district::tableID;
    storage.district_key = tpcc::district::key(W_ID, D_ID);

    PwvRWKey district_rwkey;
    district_rwkey.set_table_id(districtTableID);
    district_rwkey.set_partition_id(W_ID - 1);
    district_rwkey.set_key(&storage.district_key);
    district_rwkey.set_value(&storage.district_value);

    readSet.push_back(district_rwkey);
    writeSet.push_back(district_rwkey);

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = tpcc::customer::tableID;
    storage.customer_key = tpcc::customer::key(W_ID, D_ID, C_ID);
    PwvRWKey customer_rwkey;
    customer_rwkey.set_table_id(customerTableID);
    customer_rwkey.set_partition_id(W_ID - 1);
    customer_rwkey.set_key(&storage.customer_key);
    customer_rwkey.set_value(&storage.customer_value);
    readSet.push_back(customer_rwkey);

    CHECK(readSet.size() == 3) << "Check on readSet size failed!";
    CHECK(writeSet.size() == 1) << "Check on writeSet size failed!";
  }

  void execute() override {}

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
};

class PwvNewOrderOrderStatement : public PwvStatement {
public:
  PwvNewOrderOrderStatement(tpcc::Database &db, const tpcc::Context &context,
                            tpcc::Random &random, tpcc::Storage &storage,
                            std::size_t partition_id,
                            const tpcc::NewOrderQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvNewOrderOrderStatement() override = default;

  void prepare_read_and_write_set() override {}

  void execute() override {}

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
};

class PwvNewOrderStockStatement : public PwvStatement {
public:
  PwvNewOrderStockStatement(tpcc::Database &db, const tpcc::Context &context,
                            tpcc::Random &random, tpcc::Storage &storage,
                            std::size_t partition_id,
                            const tpcc::NewOrderQuery &query, int idx)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query), idx(idx) {}

  ~PwvNewOrderStockStatement() override = default;

  void prepare_read_and_write_set() override {

    auto itemTableID = tpcc::item::tableID;
    auto stockTableID = tpcc::stock::tableID;

    // The row in the ITEM table with matching I_ID (equals OL_I_ID) is
    // selected and I_PRICE, the price of the item, I_NAME, the name of the
    // item, and I_DATA are retrieved. If I_ID has an unused value (see
    // Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
    // rollback of the database transaction (see Clause 2.4.2.3).

    int32_t OL_I_ID = query.INFO[idx].OL_I_ID;
    int8_t OL_QUANTITY = query.INFO[idx].OL_QUANTITY;
    int32_t OL_SUPPLY_W_ID = query.INFO[idx].OL_SUPPLY_W_ID;

    storage.item_keys[idx] = tpcc::item::key(OL_I_ID);

    // If I_ID has an unused value, rollback.
    // In OCC, rollback can return without going through commit protocal

    if (storage.item_keys[idx].I_ID == 0) {
      // TODO handle abort;
    }

    // TODO handle index read
    /*
    this->search_local_index(itemTableID, 0, storage.item_keys[i],
                             storage.item_values[i]);
    */

    // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
    // S_W_ID (equals OL_SUPPLY_W_ID) is selected.

    storage.stock_keys[idx] = tpcc::stock::key(OL_SUPPLY_W_ID, OL_I_ID);

    PwvRWKey stock_rwkey;
    stock_rwkey.set_table_id(stockTableID);
    stock_rwkey.set_partition_id(OL_SUPPLY_W_ID - 1);
    stock_rwkey.set_key(&storage.stock_keys[idx]);
    stock_rwkey.set_value(&storage.stock_values[idx]);
    readSet.push_back(stock_rwkey);
    writeSet.push_back(stock_rwkey);
  }

  void execute() override {}

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
  int idx;
};

class PwvPaymentDistrictStatement : public PwvStatement {
public:
  PwvPaymentDistrictStatement(tpcc::Database &db, const tpcc::Context &context,
                              tpcc::Random &random, tpcc::Storage &storage,
                              std::size_t partition_id,
                              const tpcc::PaymentQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvPaymentDistrictStatement() override = default;

  void prepare_read_and_write_set() override {

    int32_t W_ID = partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    // The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
    // D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
    // and D_YTD,

    auto districtTableID = tpcc::district::tableID;
    storage.district_key = tpcc::district::key(W_ID, D_ID);

    PwvRWKey rwkey;
    rwkey.set_table_id(districtTableID);
    rwkey.set_partition_id(W_ID - 1);
    rwkey.set_key(&storage.district_key);
    rwkey.set_value(&storage.district_value);

    readSet.push_back(rwkey);
    writeSet.push_back(rwkey);

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(writeSet.size() == 1) << "Check on writeSet size failed!";
  }

  void execute() override {}

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::PaymentQuery &query;
};

class PwvPaymentCustomerStatement : public PwvStatement {
public:
  PwvPaymentCustomerStatement(tpcc::Database &db, const tpcc::Context &context,
                              tpcc::Random &random, tpcc::Storage &storage,
                              std::size_t partition_id,
                              const tpcc::PaymentQuery &query)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvPaymentCustomerStatement() override = default;

  void prepare_read_and_write_set() override {

    int32_t W_ID = this->partition_id + 1;

    // The input data (see Clause 2.5.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int32_t C_D_ID = query.C_D_ID;
    int32_t C_W_ID = query.C_W_ID;
    float H_AMOUNT = query.H_AMOUNT;

    CHECK(C_ID > 0) << "Check on C_ID failed!";

    // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
    // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
    // customer's last name, and C_CREDIT, the customer's credit status, are
    // retrieved.

    auto customerTableID = tpcc::customer::tableID;
    storage.customer_key = tpcc::customer::key(C_W_ID, C_D_ID, C_ID);

    PwvRWKey rwkey;
    rwkey.set_table_id(customerTableID);
    rwkey.set_partition_id(C_W_ID - 1);
    rwkey.set_key(&storage.customer_key);
    rwkey.set_value(&storage.customer_value);

    readSet.push_back(rwkey);
    writeSet.push_back(rwkey);

    CHECK(readSet.size() == 1) << "Check on readSet size failed!";
    CHECK(writeSet.size() == 1) << "Check on writeSet size failed!";
  }

  void execute() override {}

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::PaymentQuery &query;
};

} // namespace scar