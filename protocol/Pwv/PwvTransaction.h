//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "protocol/Pwv/PwvStatement.h"

namespace scar {

class PwvTransaction {

public:
  virtual ~PwvTransaction() = default;

  // this commit all changes in statements that before commit points, and set
  // those statements to completed.
  virtual void commit() = 0;
};

class PwvYCSBTransaction : public PwvTransaction {
public:
  static constexpr std::size_t keys_num = 10;

  PwvYCSBTransaction(ycsb::Database &db, const ycsb::Context &context,
                     ycsb::Random &random, ycsb::Storage &storage,
                     std::size_t partition_id,
                     const ycsb::YCSBQuery<keys_num> &query, int idx)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvYCSBTransaction() override = default;

  void commit() override {}

public:
  ycsb::Database &db;
  const ycsb::Context &context;
  ycsb::Random &random;
  ycsb::Storage &storage;
  std::size_t partition_id;
  const ycsb::YCSBQuery<keys_num> &query;
};

class PwvNewOrderTransaction : public PwvTransaction {
public:
  PwvNewOrderTransaction(tpcc::Database &db, const tpcc::Context &context,
                         tpcc::Random &random, tpcc::Storage &storage,
                         std::size_t partition_id,
                         const tpcc::NewOrderQuery &query, float &total_amount)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvNewOrderTransaction() override = default;

  void commit() override {

    int32_t W_ID = partition_id + 1;

    // The input data (see Clause 2.4.3.2) are communicated to the SUT.

    int32_t D_ID = query.D_ID;
    int32_t C_ID = query.C_ID;
    int D_NEXT_O_ID = storage.district_value.D_NEXT_O_ID;

    // write to district
    {
      auto tableId = tpcc::warehouse::tableID;
      auto partitionId = W_ID - 1;
      auto key = &storage.district_key;
      auto value = &storage.district_value;
      ITable *table = db.find_table(tableId, partitionId);
      table->update(key, value);
    }

    // write to stocks
    for (int i = 0; i < query.O_OL_CNT; i++) {

      int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;
      auto tableId = tpcc::stock::tableID;
      auto partitionId = OL_SUPPLY_W_ID - 1;
      auto key = &storage.stock_keys[i];
      auto value = &storage.stock_values[i];
      ITable *table = db.find_table(tableId, partitionId);
      table->update(key, value);
    }
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
};

class PwvPaymentTransaction : public PwvTransaction {
public:
  PwvPaymentTransaction(tpcc::Database &db, const tpcc::Context &context,
                        tpcc::Random &random, tpcc::Storage &storage,
                        std::size_t partition_id,
                        const tpcc::NewOrderQuery &query, float &total_amount)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id), query(query) {}

  ~PwvPaymentTransaction() override = default;

  void commit() override {}

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::NewOrderQuery &query;
};

} // namespace scar