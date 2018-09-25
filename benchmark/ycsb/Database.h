//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Schema.h"
#include "common/Operation.h"
#include "core/Table.h"
#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <thread>
#include <unordered_map>
#include <vector>

namespace scar {
namespace ycsb {
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
      auto ycsbTableID = ycsb::tableID;
      tbl_ycsb_vec.push_back(
          std::make_unique<Table<997, ycsb::key, ycsb::value, MetaDataType>>(
              ycsbTableID, partitionID));
    }

    // there is 1 table in ycsb
    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<TableType> &table) { return table.get(); };

    std::transform(tbl_ycsb_vec.begin(), tbl_ycsb_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables("ycsb",
               [&context, this](std::size_t partitionID) {
                 ycsbInit(context, partitionID);
               },
               partitionNum, threadsNum);
  }

  void apply_operation(const Operation &operation) {
    CHECK(false); // not supported
  }

private:
  void ycsbInit(const Context &context, std::size_t partitionID) {

    Random random;
    TableType *table = tbl_ycsb_vec[partitionID].get();

    std::size_t keysPerPartition =
        context.keysPerPartition; // 5M keys per partition
    std::size_t partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;

    if (context.strategy == PartitionStrategy::RANGE) {

      // use range partitioning

      for (auto i = partitionID * keysPerPartition;
           i < (partitionID + 1) * keysPerPartition; i++) {

        DCHECK(context.getPartitionID(i) == partitionID);

        ycsb::key key(i);
        ycsb::value value;
        value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        table->insert(&key, &value);
      }

    } else {

      // use round-robin hash partitioning

      for (auto i = partitionID; i < totalKeys; i += partitionNum) {

        DCHECK(context.getPartitionID(i) == partitionID);

        ycsb::key key(i);
        ycsb::value value;
        value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        table->insert(&key, &value);
      }
    }
  }

private:
  std::vector<std::vector<TableType *>> tbl_vecs;
  std::vector<std::unique_ptr<TableType>> tbl_ycsb_vec;
};
} // namespace ycsb
} // namespace scar
