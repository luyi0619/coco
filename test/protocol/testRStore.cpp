//
// Created by Yi Lu on 9/6/18.
//
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Workload.h"
#include "core/Transaction.h"
#include "protocol/RStore/RStore.h"
#include "protocol/RStore/RStoreExecutor.h"
#include "protocol/RStore/RStoreSwitcher.h"
#include <gtest/gtest.h>

TEST(TestRStore, TestRStoreSwitcher) {

  using MetaDataType = std::atomic<uint64_t>;
  using TransactionType = scar::Transaction<scar::tpcc::Database<MetaDataType>>;
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

  scar::RStoreExecutor<WorkloadType> e(0, 0, db, context, worker_status,
                                       n_complete_workers);
}
