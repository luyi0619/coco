//
// Created by Yi Lu on 7/22/18.
//

#ifndef SCAR_YCSB_TRANSACTION_H
#define SCAR_YCSB_TRANSACTION_H

#include "glog/logging.h"

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "core/Transaction.h"

namespace scar {
namespace ycsb {

template <class Protocol>
class ReadModifyWrite : public Transaction<Database<Protocol>> {
public:
  using DatabaseType = Database<Protocol>;
  using ProtocolType = typename DatabaseType::ProtocolType;
  using RWKeyType = typename ProtocolType::RWKeyType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;

  ReadModifyWrite(DatabaseType &db, ContextType &context, RandomType &random)
      : Transaction<DatabaseType>(db, context, random) {}

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
      if (query.UPDATE[i]) {
        this->search(ycsbTableID, context.getPartitionID(key), ycsb_keys[i],
                     ycsb_values[i]);

        ycsb_values[i].Y_F01.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F02.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F03.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F04.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F05.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F06.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F07.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F08.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F09.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
        ycsb_values[i].Y_F10.assignStdString(
            random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

        this->update(ycsbTableID, context.getPartitionID(key), ycsb_keys[i],
                     ycsb_values[i]);
      } else {
        this->search(ycsbTableID, context.getPartitionID(key), ycsb_keys[i],
                     ycsb_values[i]);
      }
    }

    return TransactionResult::COMMIT;
  }
};
} // namespace ycsb

} // namespace scar

#endif // SCAR_YCSB_TRANSACTION_H
