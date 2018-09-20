//
// Created by Yi Lu on 9/19/18.
//

#include "protocol/Scar/ScarRWKey.h"
#include <gtest/gtest.h>

TEST(TestScarRWKey, TestBasic) {

  scar::SiloRWKey key;

  EXPECT_EQ(key.get_key(), nullptr);
  EXPECT_EQ(key.get_value(), nullptr);
  EXPECT_EQ(key.get_tid(), 0);
  EXPECT_EQ(key.get_table_id(), 0);
  EXPECT_EQ(key.get_partition_id(), 0);
  EXPECT_EQ(key.get_local_index_read_bit(), false);
  EXPECT_EQ(key.get_write_lock_bit(), false);
  EXPECT_EQ(key.get_read_request_bit(), false);
  EXPECT_EQ(key.get_read_validation_success_bit(), false);
  EXPECT_EQ(key.get_wts_change_in_read_validation_bit(), false);

  key.set_local_index_read_bit();
  key.set_write_lock_bit();
  key.set_read_request_bit();
  key.set_read_validation_success_bit();
  key.set_wts_change_in_read_validation_bit();
  key.set_table_id(23);
  key.set_partition_id(45);

  EXPECT_EQ(key.get_table_id(), 23);
  EXPECT_EQ(key.get_partition_id(), 45);
  EXPECT_EQ(key.get_local_index_read_bit(), true);
  EXPECT_EQ(key.get_write_lock_bit(), true);
  EXPECT_EQ(key.get_read_request_bit(), true);
  EXPECT_EQ(key.get_read_validation_success_bit(), true);
  EXPECT_EQ(key.get_wts_change_in_read_validation_bit(), true);

  key.clear_local_index_read_bit();
  key.clear_write_lock_bit();
  key.clear_read_request_bit();
  key.clear_read_validation_success_bit();
  key.clear_wts_change_in_read_validation_bit();
  key.set_table_id(12);
  key.set_partition_id(78);

  EXPECT_EQ(key.get_table_id(), 12);
  EXPECT_EQ(key.get_partition_id(), 78);
  EXPECT_EQ(key.get_local_index_read_bit(), false);
  EXPECT_EQ(key.get_write_lock_bit(), false);
  EXPECT_EQ(key.get_read_request_bit(), false);
  EXPECT_EQ(key.get_read_validation_success_bit(), false);
  EXPECT_EQ(key.get_wts_change_in_read_validation_bit(), false);

}