//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Schema.h"
#include "core/Transaction.h"

namespace scar {
namespace ycsb {

struct Storage {
  ycsb::key ycsb_keys[YCSB_FIELD_SIZE];
  ycsb::value ycsb_values[YCSB_FIELD_SIZE];
};

template <class RWKey, class Database>
class ReadModifyWrite : public Transaction<RWKey, Database> {

public:
  using RWKeyType = RWKey;
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;
  using TableType = ITable<MetaDataType>;
  using StorageType = Storage;

  ReadModifyWrite(std::size_t coordinator_id, std::size_t worker_id,
                  DatabaseType &db, ContextType &context, RandomType &random,
                  Partitioner &partitioner, Storage &storage)
      : Transaction<RWKey, Database>(coordinator_id, worker_id, db, context,
                                     random, partitioner),
        storage(storage) {}

  virtual ~ReadModifyWrite() override = default;

  TransactionResult execute() override {
    ContextType &context = this->context;
    RandomType &random = this->random;
    auto partitionID = random.uniform_dist(0, context.partitionNum - 1);
    YCSBQuery<YCSB_FIELD_SIZE> query =
        makeYCSBQuery<YCSB_FIELD_SIZE>()(context, partitionID, random);

    DCHECK(context.keysPerTransaction == YCSB_FIELD_SIZE);

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0; i < YCSB_FIELD_SIZE; i++) {
      auto key = query.Y_KEY[i];
      storage.ycsb_keys[i].Y_KEY = key;
      this->search(ycsbTableID, context.getPartitionID(key),
                   storage.ycsb_keys[i], storage.ycsb_values[i]);
    }

    this->process_read_request();

    for (auto i = 0; i < YCSB_FIELD_SIZE; i++) {
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

private:
  Storage &storage;
};
} // namespace ycsb

} // namespace scar
