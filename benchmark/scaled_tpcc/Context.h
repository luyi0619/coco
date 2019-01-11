//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

namespace scar {
namespace scaled_tpcc {

enum class SCALED_TPCCWorkloadType { NEW_ORDER_ONLY, PAYMENT_ONLY, MIXED };

class Context : public scar::Context {
public:
  SCALED_TPCCWorkloadType workloadType =
      SCALED_TPCCWorkloadType::NEW_ORDER_ONLY;

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

  int n_district = 10;
  int newOrderCrossPartitionProbability = 10; // out of 100
  int paymentCrossPartitionProbability = 15;  // out of 100
};
} // namespace scaled_tpcc
} // namespace scar
