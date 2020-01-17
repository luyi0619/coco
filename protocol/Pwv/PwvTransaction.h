//
// Created by Yi Lu on 1/14/20.
//

#pragma once

#include "protocol/Pwv/PwvStatement.h"

namespace scar {

class PwvTransaction {

public:
  virtual ~PwvTransaction() = default;

  virtual void build_pieces() = 0;

  virtual void execute() = 0;

public:
  std::vector<std::unique_ptr<PwvStatement>> pieces;
};

class PwvYCSBTransaction : public PwvTransaction {
public:
  static constexpr std::size_t keys_num = 10;

  PwvYCSBTransaction(ycsb::Database &db, const ycsb::Context &context,
                     ycsb::Random &random, ycsb::Storage &storage,
                     std::size_t partition_id)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(ycsb::makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

  ~PwvYCSBTransaction() override = default;

  void build_pieces() override {
    for (int i = 0; i < keys_num; i++) {
      auto p = std::make_unique<PwvYCSBStatement>(db, context, random, storage,
                                                  partition_id, query, i);
      p->prepare_read_and_write_set();
      pieces.push_back(std::move(p));
    }
  }

  void execute() override {
    for (int i = 0; i < pieces.size(); i++) {
      if (pieces[i]->piece_partition_id() == partition_id) {
        pieces[i]->execute();
      }
    }
  }

public:
  ycsb::Database &db;
  const ycsb::Context &context;
  ycsb::Random &random;
  ycsb::Storage &storage;
  std::size_t partition_id;
  const ycsb::YCSBQuery<keys_num> query;
};

class PwvNewOrderTransaction : public PwvTransaction {
public:
  PwvNewOrderTransaction(tpcc::Database &db, const tpcc::Context &context,
                         tpcc::Random &random, tpcc::Storage &storage,
                         std::size_t partition_id)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(tpcc::makeNewOrderQuery()(context, partition_id + 1, random)) {}

  ~PwvNewOrderTransaction() override = default;

  void build_pieces() override {
    total_amount = 0;
    // init commit rvp to query.O_OL_CNT
    commit_rvp.store(query.O_OL_CNT);
    for (int i = 0; i < query.O_OL_CNT; i++) {
      auto stock_piece = std::make_unique<PwvNewOrderStockStatement>(
          db, context, random, storage, partition_id, query, i, commit_rvp);
      stock_piece->prepare_read_and_write_set();
      pieces.push_back(std::move(stock_piece));
    }
    auto warehouse_piece = std::make_unique<PwvNewOrderWarehouseStatement>(
        db, context, random, storage, partition_id, query);
    warehouse_piece->prepare_read_and_write_set();
    pieces.push_back(std::move(warehouse_piece));
    auto order_piece = std::make_unique<PwvNewOrderOrderStatement>(
        db, context, random, storage, partition_id, query, total_amount);
    order_piece->prepare_read_and_write_set();
    pieces.push_back(std::move(order_piece));
  }

  void execute() override {

    // run stocks
    int k = 0;
    while (k < query.O_OL_CNT) {
      if (pieces[k]->piece_partition_id() == partition_id) {
        pieces[k]->execute();
      }
      k++;
    }

    bool abort = false;
    for (;;) {
      int rvp = commit_rvp.load();
      if (rvp < 0) {
        abort = true;
        break;
      }
      if (rvp == 0) {
        break;
      }
      std::this_thread::yield();
    }

    if (!abort) {
      // run district
      pieces[k]->execute();
      // run order
      pieces[k + 1]->execute();
    }
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  float total_amount;
  std::atomic<int> commit_rvp;
  const tpcc::NewOrderQuery query;
};

class PwvPaymentTransaction : public PwvTransaction {
public:
  PwvPaymentTransaction(tpcc::Database &db, const tpcc::Context &context,
                        tpcc::Random &random, tpcc::Storage &storage,
                        std::size_t partition_id)
      : db(db), context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(tpcc::makePaymentQuery()(context, partition_id + 1, random)) {}

  ~PwvPaymentTransaction() override = default;

  void build_pieces() override {
    auto district_piece = std::make_unique<PwvPaymentDistrictStatement>(
        db, context, random, storage, partition_id, query);
    district_piece->prepare_read_and_write_set();
    pieces.push_back(std::move(district_piece));
    auto customer_piece = std::make_unique<PwvPaymentCustomerStatement>(
        db, context, random, storage, partition_id, query);
    customer_piece->prepare_read_and_write_set();
    pieces.push_back(std::move(customer_piece));
  }

  void execute() override {
    for (int i = 0; i < pieces.size(); i++) {
      if (pieces[i]->piece_partition_id() == partition_id) {
        pieces[i]->execute();
      }
    }
  }

public:
  tpcc::Database &db;
  const tpcc::Context &context;
  tpcc::Random &random;
  tpcc::Storage &storage;
  std::size_t partition_id;
  const tpcc::PaymentQuery query;
};

} // namespace scar