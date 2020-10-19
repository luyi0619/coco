//
// Created by Yi Lu on 7/19/18.
//

#include "benchmark/tpcc/Query.h"
#include <gtest/gtest.h>

TEST(TestTPCCQuery, TestNewOrder) {

  coco::tpcc::Context context;

  context.partition_num = 10;
  context.worker_num = 10;

  coco::tpcc::Random random(reinterpret_cast<uint64_t>(&context));

  constexpr int N = 10000, M = 10;
  constexpr int W_ID = 1;

  coco::tpcc::NewOrderQuery query =
      coco::tpcc::makeNewOrderQuery()(context, 1, random);

  EXPECT_EQ(query.W_ID, 1);
}

TEST(TestTPCCQuery, TestPayment) {

  coco::tpcc::Context context;

  context.partition_num = 10;
  context.worker_num = 10;

  coco::tpcc::Random random(reinterpret_cast<uint64_t>(&context));

  constexpr int N = 10000, M = 10;
  constexpr int W_ID = 1;

  coco::tpcc::PaymentQuery query =
      coco::tpcc::makePaymentQuery()(context, 1, random);

  EXPECT_EQ(query.W_ID, 1);
}