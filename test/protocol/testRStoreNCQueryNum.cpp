//
// Created by Yi Lu on 9/21/18.
//

#include "protocol/RStoreNC/RStoreNCQueryNum.h"
#include <gtest/gtest.h>

TEST(TestRStoreNCQueryNum, TestRStoreBasic) {

  scar::tpcc::Context context;
  context.batch_size = 1000;
  context.coordinator_num = 4;
  context.worker_num = 3;
  context.partition_num = 9;
  context.workloadType = scar::tpcc::TPCCWorkloadType::MIXED;
  context.newOrderCrossPartitionProbability = 10;
  context.paymentCrossPartitionProbability = 15;

  EXPECT_EQ(scar::RStoreNCQueryNum<scar::tpcc::Context>::get_s_phase_query_num(
                context),
            875);
  EXPECT_EQ(scar::RStoreNCQueryNum<scar::tpcc::Context>::get_c_phase_query_num(
                context),
            375);

  context.workloadType = scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;

  EXPECT_EQ(scar::RStoreNCQueryNum<scar::tpcc::Context>::get_s_phase_query_num(
                context),
            900);
  EXPECT_EQ(scar::RStoreNCQueryNum<scar::tpcc::Context>::get_c_phase_query_num(
                context),
            300);

  context.workloadType = scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY;

  EXPECT_EQ(scar::RStoreNCQueryNum<scar::tpcc::Context>::get_s_phase_query_num(
                context),
            850);
  EXPECT_EQ(scar::RStoreNCQueryNum<scar::tpcc::Context>::get_c_phase_query_num(
                context),
            450);
}
