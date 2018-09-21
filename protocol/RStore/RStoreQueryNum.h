//
// Created by Yi Lu on 9/21/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Context.h"

namespace scar {
template <class Context> class RStoreQueryNum {};

template <> class RStoreQueryNum<scar::tpcc::Context> {
public:
  static std::size_t get_s_phase_query_num(const scar::tpcc::Context &context) {
    auto total_query = context.batch_size * context.partition_num;
    if (context.workloadType == scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
      auto s_phase_new_order =
          total_query * (100 - context.newOrderCrossPartitionProbability) /
          100.0;
      return s_phase_new_order / context.partition_num;
    } else if (context.workloadType ==
               scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY) {
      auto s_phase_payment = total_query *
                             (100 - context.paymentCrossPartitionProbability) /
                             100.0;
      return s_phase_payment / context.partition_num;
    } else {
      auto s_phase_new_order =
          total_query / 2 * (100 - context.newOrderCrossPartitionProbability) /
          100.0;
      auto s_phase_payment = total_query / 2 *
                             (100 - context.paymentCrossPartitionProbability) /
                             100.0;
      return (s_phase_new_order + s_phase_payment) / context.partition_num;
    }
  }
  static std::size_t get_c_phase_query_num(const scar::tpcc::Context &context) {
    auto total_query = context.batch_size * context.partition_num;
    if (context.workloadType == scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
      auto s_phase_new_order =
          total_query * context.newOrderCrossPartitionProbability / 100.0;
      return s_phase_new_order / context.worker_num;
    } else if (context.workloadType ==
               scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY) {
      auto s_phase_payment =
          total_query * context.paymentCrossPartitionProbability / 100.0;
      return s_phase_payment / context.worker_num;
    } else {
      auto s_phase_new_order =
          total_query / 2 * context.newOrderCrossPartitionProbability / 100.0;
      auto s_phase_payment =
          total_query / 2 * context.paymentCrossPartitionProbability / 100.0;
      return (s_phase_new_order + s_phase_payment) / context.worker_num;
    }
  }
};

template <> class RStoreQueryNum<scar::ycsb::Context> {
public:
  static std::size_t get_s_phase_query_num(const scar::ycsb::Context &context) {
    auto total_query = context.batch_size * context.partition_num;
    auto s_phase_query =
        total_query * (100 - context.crossPartitionProbability) / 100.0;
    return s_phase_query / context.partition_num;
  }
  static std::size_t get_c_phase_query_num(const scar::ycsb::Context &context) {
    auto total_query = context.batch_size * context.partition_num;
    auto c_phase_query =
        total_query * context.crossPartitionProbability / 100.0;
    return c_phase_query / context.worker_num;
  }
};
} // namespace scar
