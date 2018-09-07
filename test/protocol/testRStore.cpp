//
// Created by Yi Lu on 9/6/18.
//
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Transaction.h"
#include "protocol/RStore/RStore.h"
#include "protocol/RStore/RStoreSwitcher.h"
#include <gtest/gtest.h>

TEST(TestRStore, TestRStoreRWKey) {

  scar::RStoreRWKey key;

  EXPECT_EQ(key.get_key(), nullptr);
  EXPECT_EQ(key.get_value(), nullptr);
  EXPECT_EQ(key.get_sort_key(), nullptr);
  EXPECT_EQ(key.get_tid(), 0);
  EXPECT_EQ(key.get_table_id(), 0);
  EXPECT_EQ(key.get_partition_id(), 0);
  EXPECT_EQ(key.get_lock_bit(), false);

  key.set_lock_bit();
  key.set_table_id(23);
  key.set_partition_id(45);

  EXPECT_EQ(key.get_table_id(), 23);
  EXPECT_EQ(key.get_partition_id(), 45);
  EXPECT_EQ(key.get_lock_bit(), true);

  key.clear_lock_bit();
  key.set_table_id(12);
  key.set_partition_id(78);

  EXPECT_EQ(key.get_table_id(), 12);
  EXPECT_EQ(key.get_partition_id(), 78);
  EXPECT_EQ(key.get_lock_bit(), false);
}

TEST(TestRStore, TestRStoreSwitcher) {

  using MetaDataType = std::atomic<uint64_t>;
  using TransactionType =
      scar::Transaction<scar::RStoreRWKey, scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<TransactionType>;

  scar::tpcc::Context context;
  context.coordinatorNum = 2;
  context.partitionNum = 4;
  context.workerNum = 4;
  scar::tpcc::Random random;

  scar::tpcc::Database<MetaDataType> db;

  std::atomic<bool> stopFlag;
  std::atomic<uint32_t> worker_status;
  std::atomic<uint32_t> n_complete_workers;

  scar::RStoreSwitcher<WorkloadType> switcher(
      0, 0, context, stopFlag, worker_status, n_complete_workers);
}
