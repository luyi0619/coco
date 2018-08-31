//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "core/Table.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace scar {

enum class TransactionResult { COMMIT, ABORT, ABORT_NORETRY };

template <class Protocol> class Transaction {
public:
  using ProtocolType = Protocol;
  using RWKeyType = typename Protocol::RWKeyType;
  using DatabaseType = typename Protocol::DatabaseType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MetaDataType = typename DatabaseType::MetaDataType;
  using TableType = ITable<MetaDataType>;

  static_assert(
      std::is_same<MetaDataType, typename Protocol::MetaDataType>::value,
      "The database datatype is different from the one in protocol.");

  Transaction(DatabaseType &db, ContextType &context, RandomType &random,
              ProtocolType &protocol)
      : startTime(std::chrono::steady_clock::now()), commitEpoch(0), db(db),
        context(context), random(random), protocol(protocol) {}

  virtual ~Transaction() = default;

  virtual TransactionResult execute() = 0;

  TransactionResult commit() {
    if (protocol.commit(readSet, writeSet, commitEpoch)) {
      return TransactionResult::COMMIT;
    } else {
      return TransactionResult::ABORT;
    }
  }

  template <class KeyType, class ValueType>
  void search(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, ValueType &value) {
    auto readKey = protocol.search(table_id, partition_id, key, value);
    add_to_read_set(readKey);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    auto writeKey = protocol.update(table_id, partition_id, key, value);
    add_to_write_set(writeKey);
  }

  std::size_t add_to_read_set(const RWKeyType &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const RWKeyType &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

public:
  std::chrono::steady_clock::time_point startTime;
  uint64_t commitEpoch;

protected:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  ProtocolType &protocol;
  std::vector<RWKeyType> readSet, writeSet;
};

} // namespace scar
