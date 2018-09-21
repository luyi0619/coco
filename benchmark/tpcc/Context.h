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
    return c;
  }

  Context get_cross_partition_context() const {
    Context c = *this;
    c.newOrderCrossPartitionProbability = 100;
    c.paymentCrossPartitionProbability = 100;
    return c;
  }

  int newOrderCrossPartitionProbability = 10; // out of 100
  int paymentCrossPartitionProbability = 15;  // out of 100
};
} // namespace tpcc
} // namespace scar
