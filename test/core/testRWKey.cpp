//
// Created by Yi Lu on 7/19/18.
//

#include "core/RWKey.h"
#include <gtest/gtest.h>

TEST(TestRWKey, TestBasic) {

  scar::RWKey key;

  EXPECT_EQ(key.get_key(), nullptr);
  EXPECT_EQ(key.get_value(), nullptr);
  EXPECT_EQ(key.get_sort_key(), nullptr);
  EXPECT_EQ(key.get_tid(), 0);
  EXPECT_EQ(key.get_table_id(), 0);
  EXPECT_EQ(key.get_partition_id(), 0);
  EXPECT_EQ(key.get_write_lock_bit(), false);
  EXPECT_EQ(key.get_read_request_bit(), false);

  key.set_write_lock_bit();
  key.set_read_request_bit();
  key.set_table_id(23);
  key.set_partition_id(45);

  EXPECT_EQ(key.get_table_id(), 23);
  EXPECT_EQ(key.get_partition_id(), 45);
  EXPECT_EQ(key.get_write_lock_bit(), true);
  EXPECT_EQ(key.get_read_request_bit(), true);

  key.clear_write_lock_bit();
  key.clear_read_request_bit();
  key.set_table_id(12);
  key.set_partition_id(78);

  EXPECT_EQ(key.get_table_id(), 12);
  EXPECT_EQ(key.get_partition_id(), 78);
  EXPECT_EQ(key.get_write_lock_bit(), false);
  EXPECT_EQ(key.get_read_request_bit(), false);
}