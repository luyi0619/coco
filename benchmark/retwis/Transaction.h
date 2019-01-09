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

/*
 * In this benchmark, all blind writes are updates.
 *
 */

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
        partition_id(partition_id),
        query(makeAddUserQuery()(context, partition_id, random)) {}

  virtual ~AddUser() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    RandomType random;

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < 3; i++) {
      auto key = query.KEY[i];
      storage.keys[i] = query.KEY[i];
      this->search_for_update(retwisTableID, context.getPartitionID(key),
                              storage.keys[i], storage.values[i]);
    }

    if (this->process_requests(worker_id)) {
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

  void reset_query() override {
    query = makeAddUserQuery()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
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
        partition_id(partition_id),
        query(makeFollowUnfollowQuery()(context, partition_id, random)) {}

  virtual ~FollowUnfollow() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    RandomType random;

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < 2; i++) {
      auto key = query.KEY[i];
      storage.keys[i] = query.KEY[i];
      this->search_for_update(retwisTableID, context.getPartitionID(key),
                              storage.keys[i], storage.values[i]);
    }

    if (this->process_requests(worker_id)) {
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

  void reset_query() override {
    query = makeFollowUnfollowQuery()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
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
        partition_id(partition_id),
        query(makePostTweetQuery()(context, partition_id, random)) {}

  virtual ~PostTweet() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    RandomType random;

    int retwisTableID = retwis::tableID;

    for (auto i = 0u; i < 5; i++) {
      auto key = query.KEY[i];
      storage.keys[i] = query.KEY[i];
      this->search_for_update(retwisTableID, context.getPartitionID(key),
                              storage.keys[i], storage.values[i]);
    }

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    for (auto i = 0u; i < 5; i++) {
      auto key = query.KEY[i];
      storage.values[i].VALUE.assign(random.a_string(RETWIS_SIZE, RETWIS_SIZE));
      this->update(retwisTableID, context.getPartitionID(key), storage.keys[i],
                   storage.values[i]);
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makePostTweetQuery()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
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
        partition_id(partition_id),
        query(makeGetTimelineQuery()(context, partition_id, random)) {}

  virtual ~GetTimeline() override = default;

  TransactionResult execute(std::size_t worker_id) override {

    int retwisTableID = retwis::tableID;

    for (auto i = 0; i < query.N; i++) {
      auto key = query.KEY[i];
      storage.keys[i].KEY = key;
      this->search_for_read(retwisTableID, context.getPartitionID(key),
                            storage.keys[i], storage.values[i]);
    }

    if (this->process_requests(worker_id)) {
      return TransactionResult::ABORT;
    }

    return TransactionResult::READY_TO_COMMIT;
  }

  void reset_query() override {
    query = makeGetTimelineQuery()(context, partition_id, random);
  }

private:
  DatabaseType &db;
  const ContextType &context;
  RandomType &random;
  Storage &storage;
  std::size_t partition_id;
  GetTimelineQuery query;
};

} // namespace retwis
} // namespace scar