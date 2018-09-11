//
// Created by Yi Lu on 9/6/18.
//
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Transaction.h"
#include "protocol/RStore/RStore.h"
#include "protocol/RStore/RStoreExecutor.h"
#include "protocol/RStore/RStoreManager.h"
#include <gtest/gtest.h>

TEST(TestRStore, TestRStoreSwitcher) {

  using MetaDataType = std::atomic<uint64_t>;
  using TransactionType = scar::Transaction<scar::tpcc::Database<MetaDataType>>;
  using WorkloadType = scar::tpcc::Workload<TransactionType>;

  scar::tpcc::Context context;
  context.coordinator_num = 2;
  context.partition_num = 4;
  context.worker_num = 4;
  context.protocol = "RStore";
  scar::tpcc::Random random;

  scar::tpcc::Database<MetaDataType> db;

  std::atomic<bool> stopFlag;

  scar::RStoreManager manager(0, 0, context, stopFlag);
  scar::RStoreExecutor<WorkloadType> e(0, 0, db, context, manager.worker_status,
                                       manager.n_completed_workers,
                                       manager.n_started_workers);
}
