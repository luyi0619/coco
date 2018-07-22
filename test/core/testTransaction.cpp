//
// Created by Yi Lu on 7/19/18.
//

#include <gtest/gtest.h>
#include "core/Transaction.h"
#include "protocol/Silo.h"


TEST(TestTransaction, TestBasic) {
    scar::Transaction<scar::Silo> t;
    EXPECT_EQ(true, true);
}