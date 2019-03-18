//
// Created by Yi Lu on 9/21/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Context.h"

namespace scar {
template <class Context> class StarQueryNum {

public:
  static std::size_t get_s_phase_query_num(const Context &context) {
    CHECK(false) << "not supported.";
    return 0;
  }

  static std::size_t get_c_phase_query_num(const Context &context) {
    CHECK(false) << "not supported.";
    return 0;
  }
};

template <> class StarQueryNum<scar::tpcc::Context> {
public:
  static std::size_t get_s_phase_query_num(const scar::tpcc::Context &context) {
    if (context.workloadType == scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
      return context.batch_size *
             (100 - context.newOrderCrossPartitionProbability) / 100;
    } else if (context.workloadType ==
               scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY) {
      return context.batch_size *
             (100 - context.paymentCrossPartitionProbability) / 100;
    } else {
      return (context.batch_size *
                  (100 - context.newOrderCrossPartitionProbability) / 100 +
              context.batch_size *
                  (100 - context.paymentCrossPartitionProbability) / 100) /
             2;
    }
  }

  static std::size_t get_c_phase_query_num(const scar::tpcc::Context &context) {
    if (context.workloadType == scar::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
      return context.coordinator_num * context.batch_size *
             context.newOrderCrossPartitionProbability / 100;
    } else if (context.workloadType ==
               scar::tpcc::TPCCWorkloadType::PAYMENT_ONLY) {
      return context.coordinator_num * context.batch_size *
             context.paymentCrossPartitionProbability / 100;
    } else {
      return context.coordinator_num *
             (context.batch_size * context.newOrderCrossPartitionProbability /
                  100 +
              context.batch_size * context.paymentCrossPartitionProbability /
                  100) /
             2;
    }
  }
};

template <> class StarQueryNum<scar::ycsb::Context> {
public:
  static std::size_t get_s_phase_query_num(const scar::ycsb::Context &context) {
    return context.batch_size * (100 - context.crossPartitionProbability) / 100;
  }

  static std::size_t get_c_phase_query_num(const scar::ycsb::Context &context) {
    return context.coordinator_num * context.batch_size *
           context.crossPartitionProbability / 100;
  }
};
} // namespace scar