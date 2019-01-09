//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Schema.h"
#include "benchmark/ycsb/Storage.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace scar {
namespace ycsb {

template <class Transaction> class ReadModifyWrite : public Transaction {

public:
  using MetaDataType = typename Transaction::MetaDataType;
  using DatabaseType = Database<MetaDataType>;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using TableType = ITable<MetaDataType>;
  using StorageType = Storage;

  static constexpr std::size_t keys_num = 10;

  ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id,
                  DatabaseType &db, const ContextType &context,
                  RandomType &random, Partitioner &partitioner,
                  Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        partition_id(partition_id),
        query(makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

  virtual ~ReadModifyWrite() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    RandomType random;

    DCHECK(context.keysPerTransaction == keys_num);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;
      if (query.UPDATE[i]) {
        this->search_for_update(ycsbTableID, context.getPartitionID(key),
                                storage.ycsb_keys[i], storage.ycsb_values[i]);
      } else {
        this->search_for_read(ycsbTableID, context.getPartitionID(key),
                              storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < keys_num; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {
        storage.ycsb_values[i].Y_F01.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F02.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F03.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F04.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F05.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F06.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F07.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F08.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F09.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        storage.ycsb_values[i].Y_F10.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        this->update(ycsbTableID, context.getPartitionID(key),
                     storage.ycsb_keys[i], storage.ycsb_values[i]);
      }
    }
    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeYCSBQuery<keys_num>()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  YCSBQuery<keys_num> query;
};
} // namespace ycsb

} // namespace scar
