//
// Created by Yi Lu on 9/21/18.
//

#include "benchmark/tpcc/Context.h"
#include "protocol/RStore/RStoreQueryNum.h"
#include <gtest/gtest.h>

TEST(TestRStoreQueryNum, TestRStoreBasic) {

  scar::tpcc::Context context;
  context.batch_size = 1000;
  context.coordinator_num = 4;
  context.worker_num = 1;
  context.partition_num = 4;
  context.workloadType = scar::tpcc::TPCCWorkloadType::MIXED;
  context.newOrderCrossPartitionProbability = 10;
  context.paymentCrossPartitionProbability = 15;

  EXPECT_EQ(
      scar::RStoreQueryNum<scar::tpcc::Context>::get_s_phase_query_num(context),
      875);
  EXPECT_EQ(
      scar::RStoreQueryNum<scar::tpcc::Context>::get_c_phase_query_num(context),
      500);

  context.workloadType = scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;

  EXPECT_EQ(
      scar::RStoreQueryNum<scar::tpcc::Context>::get_s_phase_query_num(context),
      900);
  EXPECT_EQ(
      scar::RStoreQueryNum<scar::tpcc::Context>::get_c_phase_query_num(context),
      400);

  context.workloadType = scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY;

  EXPECT_EQ(
      scar::RStoreQueryNum<scar::tpcc::Context>::get_s_phase_query_num(context),
      850);
  EXPECT_EQ(
      scar::RStoreQueryNum<scar::tpcc::Context>::get_c_phase_query_num(context),
      600);
}
