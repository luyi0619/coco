//
// Created by Yi Lu on 7/19/18.
//

#ifndef SCAR_TPCC_CONTEXT_H
#define SCAR_TPCC_CONTEXT_H

#include "core/Context.h"

namespace scar {
    namespace tpcc {

        enum class TPCCWorkloadType {
            NEW_ORDER_ONLY, PAYMENT_ONLY, MIXED
        };

        class Context : public scar::Context {
        public:

            TPCCWorkloadType workloadType = TPCCWorkloadType::MIXED;

            int newOrderCrossPartitionProbability = 10; // out of 100
            int paymentCrossPartitionProbability = 15; // out of 100
        };
    }
}


#endif //SCAR_TPCC_CONTEXT_H
