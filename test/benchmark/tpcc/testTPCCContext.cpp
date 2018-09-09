//
// Created by Yi Lu on 9/9/18.
//

#include "benchmark/tpcc/Context.h"
#include "glog/logging.h"
#include <gtest/gtest.h>

TEST(TestTPCCContext, TestBasic) {
  scar::tpcc::Context context;
  context.batch_query = 1000;
  context.coordinator_num = 4;
  context.worker_num = 1;
  context.partition_num = 4;
  context.workloadType = scar::tpcc::TPCCWorkloadType::MIXED;

  EXPECT_EQ(context.get_s_phase_query_num(), 875);
  EXPECT_EQ(context.get_c_phase_query_num(), 500);

  context.workloadType = scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;

  EXPECT_EQ(context.get_s_phase_query_num(), 900);
  EXPECT_EQ(context.get_c_phase_query_num(), 400);

  context.workloadType = scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY;

  EXPECT_EQ(context.get_s_phase_query_num(), 850);
  EXPECT_EQ(context.get_c_phase_query_num(), 600);
}