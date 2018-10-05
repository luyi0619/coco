//
// Created by Yi Lu on 10/1/18.
//

#pragma once

#include "benchmark/retwis/Context.h"
#include "benchmark/retwis/Database.h"
#include "benchmark/retwis/Random.h"
#include "benchmark/retwis/Storage.h"
#include "benchmark/retwis/Transaction.h"
#include "core/Partitioner.h"

namespace scar {

namespace retwis {

template <class Transaction> class Workload {
public:
  using TransactionType = Transaction;
  using DatabaseType = Database<typename TransactionType::MetaDataType>;
  using ContextType = Context;
  using RandomType = Random;
  using StorageType = Storage;

  Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
           Partitioner &partitioner)
      : coordinator_id(coordinator_id), db(db), random(random),
        partitioner(partitioner) {}

  std::unique_ptr<TransactionType> next_transaction(const ContextType &context,
                                                    std::size_t partition_id,
                                                    StorageType &storage) {

    int x = random.uniform_dist(1, 100);
    std::unique_ptr<TransactionType> p;

    if (x <= 5) {
      p = std::make_unique<AddUser<Transaction>>(coordinator_id, partition_id,
                                                 db, context, random,
                                                 partitioner, storage);
    } else if (x <= 10) {
      p = std::make_unique<FollowUnfollow<Transaction>>(
          coordinator_id, partition_id, db, context, random, partitioner,
          storage);
    } else if (x <= 20) {
      p = std::make_unique<PostTweet<Transaction>>(coordinator_id, partition_id,
                                                   db, context, random,
                                                   partitioner, storage);
    } else {
      p = std::make_unique<GetTimeline<Transaction>>(
          coordinator_id, partition_id, db, context, random, partitioner,
          storage);
    }
    return p;
  }

private:
  std::size_t coordinator_id;
  DatabaseType &db;
  RandomType &random;
  Partitioner &partitioner;
};

} // namespace retwis
} // namespace scar
