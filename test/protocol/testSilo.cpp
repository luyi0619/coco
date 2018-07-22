//
// Created by Yi Lu on 7/19/18.
//

#include <gtest/gtest.h>
#include "protocol/Silo.h"


TEST(TestSilo, TestSiloRWKey) {

    scar::SiloRWKey key;

    EXPECT_EQ(key.get_key(), nullptr);
    EXPECT_EQ(key.get_value(), nullptr);
    EXPECT_EQ(key.get_sort_key(), nullptr);
    EXPECT_EQ(key.get_tid(), 0);
    EXPECT_EQ(key.get_table_id(), 0);
    EXPECT_EQ(key.get_partition_id(), 0);
    EXPECT_EQ(key.is_lock_bit(), false);

    key.set_table_id(123);
    key.set_lock_bit();
    key.set_partition_id(0x45678);

    EXPECT_EQ(key.get_table_id(), 123);
    EXPECT_EQ(key.get_partition_id(), 0x45678);
    EXPECT_EQ(key.is_lock_bit(), true);

    key.set_table_id(0x1234);
    key.clear_lock_bit();
    key.set_partition_id(0x1ffff);


    EXPECT_EQ(key.get_table_id(), 0x1234);
    EXPECT_EQ(key.get_partition_id(), 0x1ffff);
    EXPECT_EQ(key.is_lock_bit(), false);
}