//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "benchmark/retwis/Context.h"
#include "benchmark/retwis/Random.h"
#include "benchmark/retwis/Schema.h"
#include "benchmark/retwis/Storage.h"
#include "common/Operation.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <glog/logging.h>
#include <thread>
#include <unordered_map>
#include <vector>

namespace scar {
namespace retwis {
class Database {
public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(partition_id < tbl_vecs[table_id].size());
    return tbl_vecs[table_id][partition_id];
  }

  template <class InitFunc>
  void initTables(const std::string &name, InitFunc initFunc,
                  std::size_t partitionNum, std::size_t threadsNum,
                  Partitioner *partitioner) {

    std::vector<int> all_parts;

    for (auto i = 0u; i < partitionNum; i++) {
      if (partitioner == nullptr ||
          partitioner->is_partition_replicated_on_me(i)) {
        all_parts.push_back(i);
      }
    }

    std::vector<std::thread> v;
    auto now = std::chrono::steady_clock::now();

    for (auto threadID = 0u; threadID < threadsNum; threadID++) {
      v.emplace_back([=]() {
        for (auto i = threadID; i < all_parts.size(); i += threadsNum) {
          auto partitionID = all_parts[i];
          initFunc(partitionID);
        }
      });
    }
    for (auto &t : v) {
      t.join();
    }
    LOG(INFO) << name << " initialization finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
  }

  void initialize(const Context &context) {

    std::size_t coordinator_id = context.coordinator_id;
    std::size_t partitionNum = context.partition_num;
    std::size_t threadsNum = context.worker_num;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto retwisTableID = retwis::tableID;
      tbl_retwis_vec.push_back(
          TableFactory::create_table<9973, retwis::key, retwis::value>(
              context, retwisTableID, partitionID));
    }

    // there is 1 table in retwis
    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_retwis_vec.begin(), tbl_retwis_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables(
        "retwis",
        [&context, this](std::size_t partitionID) {
          retwisInit(context, partitionID);
        },
        partitionNum, threadsNum, partitioner.get());
  }

  void apply_operation(const Operation &operation) {
    CHECK(false); // not supported
  }

private:
  void retwisInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_retwis_vec[partitionID].get();

    std::size_t keysPerPartition =
        context.keysPerPartition; // 5M keys per partition
    std::size_t partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;

    if (context.strategy == PartitionStrategy::RANGE) {

      // use range partitioning

      for (auto i = partitionID * keysPerPartition;
           i < (partitionID + 1) * keysPerPartition; i++) {

        DCHECK(context.getPartitionID(i) == partitionID);

        retwis::key key(i);
        retwis::value value;
        value.VALUE.assign(random.a_string(RETWIS_SIZE, RETWIS_SIZE));

        table->insert(&key, &value);
      }

    } else {

      // use round-robin hash partitioning

      for (auto i = partitionID; i < totalKeys; i += partitionNum) {

        DCHECK(context.getPartitionID(i) == partitionID);

        retwis::key key(i);
        retwis::value value;
        value.VALUE.assign(random.a_string(RETWIS_SIZE, RETWIS_SIZE));

        table->insert(&key, &value);
      }
    }
  }

private:
  std::vector<std::vector<ITable *>> tbl_vecs;
  std::vector<std::unique_ptr<ITable>> tbl_retwis_vec;
};
} // namespace retwis
} // namespace scar
