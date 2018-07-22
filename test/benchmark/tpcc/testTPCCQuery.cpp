//
// Created by Yi Lu on 7/19/18.
//

#include "benchmark/tpcc/Query.h"
#include <gtest/gtest.h>

TEST(TestTPCCQuery, TestNewOrder) {

  scar::tpcc::Context context;

  context.partitionNum = 10;
  context.workerNum = 10;

  scar::tpcc::Random random(reinterpret_cast<uint64_t>(&context));

  constexpr int N = 10000, M = 10;
  constexpr int W_ID = 1;

  scar::tpcc::NewOrderQuery query =
      scar::tpcc::makeNewOrderQuery()(context, 1, random);

  EXPECT_EQ(query.W_ID, 1);
}

TEST(TestTPCCQuery, TestPayment) {

  scar::tpcc::Context context;

  context.partitionNum = 10;
  context.workerNum = 10;

  scar::tpcc::Random random(reinterpret_cast<uint64_t>(&context));

  constexpr int N = 10000, M = 10;
  constexpr int W_ID = 1;

  scar::tpcc::PaymentQuery query =
      scar::tpcc::makePaymentQuery()(context, 1, random);

  EXPECT_EQ(query.W_ID, 1);
}