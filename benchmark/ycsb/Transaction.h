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
template <class RWKey, class Database>
class ReadModifyWrite : public Transaction<RWKey, Database> {

public:
  using RWKeyType = RWKey;
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;
  using TableType = ITable<MetaDataType>;

  ReadModifyWrite(DatabaseType &db, ContextType &context, RandomType &random)
      : Transaction<RWKey, Database>(db, context, random) {}

  virtual ~ReadModifyWrite() override = default;

  TransactionResult execute() override {
    ContextType &context = this->context;
    RandomType &random = this->random;
    auto partitionID = random.uniform_dist(0, context.partitionNum - 1);
    YCSBQuery<YCSB_FIELD_SIZE> query =
        makeYCSBQuery<YCSB_FIELD_SIZE>()(context, partitionID, random);

    CHECK(context.keysPerTransaction == YCSB_FIELD_SIZE);

    ycsb::key ycsb_keys[YCSB_FIELD_SIZE];
    ycsb::value ycsb_values[YCSB_FIELD_SIZE];

    int ycsbTableID = ycsb::tableID;

    for (auto i = 0; i < YCSB_FIELD_SIZE; i++) {
      auto key = query.Y_KEY[i];
      ycsb_keys[i].Y_KEY = key;
      this->search(ycsbTableID, context.getPartitionID(key), ycsb_keys[i],
                   ycsb_values[i]);
    }

    this->process_read_request();

    for (auto i = 0; i < YCSB_FIELD_SIZE; i++) {
      auto key = query.Y_KEY[i];
      if (query.UPDATE[i]) {
        ycsb_values[i].Y_F01.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F02.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F03.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F04.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F05.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F06.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F07.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F08.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F09.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F10.assign(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        this->update(ycsbTableID, context.getPartitionID(key), ycsb_keys[i],
                     ycsb_values[i]);
      }
    }
    return TransactionResult::READY_TO_COMMIT;
  }
};
} // namespace ycsb

} // namespace scar
