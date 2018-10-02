//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/retwis/Database.h"
#include "benchmark/retwis/Query.h"
#include "benchmark/retwis/Schema.h"
#include "benchmark/retwis/Storage.h"
#include "common/Operation.h"
#include "common/Time.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace scar {
namespace retwis {

// TODO: this benchmark has blind write

template <class Transaction> class AddUser : public Transaction {
public:
  using MetaDataType = typename Transaction::MetaDataType;
  using DatabaseType = Database<MetaDataType>;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using TableType = ITable<MetaDataType>;
  using StorageType = Storage;

  AddUser(std::size_t coordinator_id, std::size_t partition_id,
          DatabaseType &db, const ContextType &context, RandomType &random,
          Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        query(makeAddUserQuery()(context, partition_id + 1, random)) {}

  virtual ~AddUser() override = default;

  TransactionResult execute() override {

    RandomType random;

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < 3; i++) {
      auto key = query.KEY[i];
      storage.keys[i] = query.KEY[i];
      this->search_for_update(retwisTableID, context.getPartitionID(key),
                              storage.keys[i], storage.values[i]);
    }

    if (this->process_requests()) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < 1; i++) {
      auto key = query.KEY[i];
      storage.values[i].VALUE.assign(random.a_string(RETWIS_SIZE, RETWIS_SIZE));
      this->update(retwisTableID, context.getPartitionID(key), storage.keys[i],
                   storage.values[i]);
    }

    return TransactionResult::READY_TO_COMMIT;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  AddUserQuery query;
};

template <class Transaction> class FollowUnfollow : public Transaction {
public:
  using MetaDataType = typename Transaction::MetaDataType;
  using DatabaseType = Database<MetaDataType>;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using TableType = ITable<MetaDataType>;
  using StorageType = Storage;

  FollowUnfollow(std::size_t coordinator_id, std::size_t partition_id,
                 DatabaseType &db, const ContextType &context,
                 RandomType &random, Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        query(makeFollowUnfollowQuery()(context, partition_id + 1, random)) {}

  virtual ~FollowUnfollow() override = default;

  TransactionResult execute() override {

    RandomType random;

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < 2; i++) {
      auto key = query.KEY[i];
      storage.keys[i] = query.KEY[i];
      this->search_for_update(retwisTableID, context.getPartitionID(key),
                              storage.keys[i], storage.values[i]);
    }

    if (this->process_requests()) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < 2; i++) {
      auto key = query.KEY[i];
      storage.values[i].VALUE.assign(random.a_string(RETWIS_SIZE, RETWIS_SIZE));
      this->update(retwisTableID, context.getPartitionID(key), storage.keys[i],
                   storage.values[i]);
    }

    return TransactionResult::READY_TO_COMMIT;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  FollowUnfollowQuery query;
};

template <class Transaction> class PostTweet : public Transaction {
public:
  using MetaDataType = typename Transaction::MetaDataType;
  using DatabaseType = Database<MetaDataType>;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using TableType = ITable<MetaDataType>;
  using StorageType = Storage;

  PostTweet(std::size_t coordinator_id, std::size_t partition_id,
            DatabaseType &db, const ContextType &context, RandomType &random,
            Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        query(makePostTweetQuery()(context, partition_id + 1, random)) {}

  virtual ~PostTweet() override = default;

  TransactionResult execute() override {

    RandomType random;

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < 5; i++) {
      auto key = query.KEY[i];
      storage.keys[i] = query.KEY[i];
      this->search_for_update(retwisTableID, context.getPartitionID(key),
                              storage.keys[i], storage.values[i]);
    }

    if (this->process_requests()) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < 3; i++) {
      auto key = query.KEY[i];
      storage.values[i].VALUE.assign(random.a_string(RETWIS_SIZE, RETWIS_SIZE));
      this->update(retwisTableID, context.getPartitionID(key), storage.keys[i],
                   storage.values[i]);
    }

    return TransactionResult::READY_TO_COMMIT;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  PostTweetQuery query;
};

template <class Transaction> class GetTimeline : public Transaction {
public:
  using MetaDataType = typename Transaction::MetaDataType;
  using DatabaseType = Database<MetaDataType>;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using TableType = ITable<MetaDataType>;
  using StorageType = Storage;

  GetTimeline(std::size_t coordinator_id, std::size_t partition_id,
              DatabaseType &db, const ContextType &context, RandomType &random,
              Partitioner &partitioner, Storage &storage)
      : Transaction(coordinator_id, partition_id, partitioner), db(db),
        context(context), random(random), storage(storage),
        query(makeGetTimelineQuery()(context, partition_id + 1, random)) {}

  virtual ~GetTimeline() override = default;

  TransactionResult execute() override {

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < query.N; i++) {
      auto key = query.KEY[i];
      storage.keys[i].KEY = key;
      this->search_for_read(retwisTableID, context.getPartitionID(key),
                            storage.keys[i], storage.values[i]);
    }

    if (this->process_requests()) {
      return TransactionResult::ABORT;
    }

    return TransactionResult::READY_TO_COMMIT;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  GetTimelineQuery query;
};

} // namespace retwis
} // namespace scar