//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

namespace scar {
namespace tpcc {

enum class TPCCWorkloadType { NEW_ORDER_ONLY, PAYMENT_ONLY, MIXED };

class Context : public scar::Context {
public:
  TPCCWorkloadType workloadType = TPCCWorkloadType::NEW_ORDER_ONLY;

  Context get_single_partition_context() const {
    Context c = *this;
    c.newOrderCrossPartitionProbability = 0;
    c.paymentCrossPartitionProbability = 0;
    c.operation_replication = this->operation_replication;
    return c;
  }

  Context get_cross_partition_context() const {
    Context c = *this;
    c.newOrderCrossPartitionProbability = 100;
    c.paymentCrossPartitionProbability = 100;
    c.operation_replication = false;
    return c;
  }

  std::size_t get_s_phase_query_num() const override {
    auto total_query = batch_size * partition_num;
    if (workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      auto s_phase_new_order =
          total_query * (100 - newOrderCrossPartitionProbability) / 100.0;
      return s_phase_new_order / partition_num;
    } else if (workloadType == TPCCWorkloadType::PAYMENT_ONLY) {
      auto s_phase_payment =
          total_query * (100 - paymentCrossPartitionProbability) / 100.0;
      return s_phase_payment / partition_num;
    } else {
      auto s_phase_new_order =
          total_query / 2 * (100 - newOrderCrossPartitionProbability) / 100.0;
      auto s_phase_payment =
          total_query / 2 * (100 - paymentCrossPartitionProbability) / 100.0;
      return (s_phase_new_order + s_phase_payment) / partition_num;
    }
  }
  std::size_t get_c_phase_query_num() const override {
    auto total_query = batch_size * partition_num;
    if (workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
      auto s_phase_new_order =
          total_query * newOrderCrossPartitionProbability / 100.0;
      return s_phase_new_order / worker_num;
    } else if (workloadType == TPCCWorkloadType::PAYMENT_ONLY) {
      auto s_phase_payment =
          total_query * paymentCrossPartitionProbability / 100.0;
      return s_phase_payment / worker_num;
    } else {
      auto s_phase_new_order =
          total_query / 2 * newOrderCrossPartitionProbability / 100.0;
      auto s_phase_payment =
          total_query / 2 * paymentCrossPartitionProbability / 100.0;
      return (s_phase_new_order + s_phase_payment) / worker_num;
    }
  }

  int newOrderCrossPartitionProbability = 10; // out of 100
  int paymentCrossPartitionProbability = 15;  // out of 100
};
} // namespace tpcc
} // namespace scar
