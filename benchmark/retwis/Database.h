//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "benchmark/retwis/Context.h"
#include "benchmark/retwis/Random.h"
#include "benchmark/retwis/Schema.h"
#include "common/Operation.h"
#include "core/Table.h"
#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <thread>
#include <unordered_map>
#include <vector>

namespace scar {
namespace retwis {
template <class MetaData> class Database {
public:
  using MetaDataType = MetaData;
  using ContextType = Context;
  using RandomType = Random;
  using TableType = ITable<MetaDataType>;

  TableType *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(partition_id < tbl_vecs[table_id].size());
    return tbl_vecs[table_id][partition_id];
  }

  template <class InitFunc>
  void initTables(const std::string &name, InitFunc initFunc,
                  std::size_t partitionNum, std::size_t threadsNum) {
    std::vector<std::thread> v;
    auto now = std::chrono::steady_clock::now();
    for (auto threadID = 0u; threadID < threadsNum; threadID++) {
      v.emplace_back([=]() {
        for (auto partitionID = threadID; partitionID < partitionNum;
             partitionID += threadsNum) {
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

  void initialize(const Context &context, std::size_t partitionNum,
                  std::size_t threadsNum) {

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto retwisTableID = retwis::tableID;
      tbl_retwis_vec.push_back(
          std::make_unique<
              Table<997, retwis::key, retwis::value, MetaDataType>>(
              retwisTableID, partitionID));
    }

    // there is 1 table in retwis
    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<TableType> &table) { return table.get(); };

    std::transform(tbl_retwis_vec.begin(), tbl_retwis_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables("retwis",
               [&context, this](std::size_t partitionID) {
                 retwisInit(context, partitionID);
               },
               partitionNum, threadsNum);
  }

  void apply_operation(const Operation &operation) {
    CHECK(false); // not supported
  }

private:
  void retwisInit(const Context &context, std::size_t partitionID) {

    Random random;
    TableType *table = tbl_retwis_vec[partitionID].get();

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
  std::vector<std::vector<TableType *>> tbl_vecs;
  std::vector<std::unique_ptr<TableType>> tbl_retwis_vec;
};
} // namespace retwis
} // namespace scar
