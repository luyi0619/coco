//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include <vector>

#include "core/Table.h"

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
      : db(db), context(context), random(random), protocol(protocol) {}

  virtual ~Transaction() = default;

  virtual TransactionResult execute() = 0;

  TransactionResult commit() {
    if (protocol.commit(readSet, writeSet)) {
      return TransactionResult::COMMIT;
    } else {
      return TransactionResult::ABORT;
    }
  }

  template <class KeyType, class ValueType>
  void search(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, ValueType &value) {
    TableType *table = db.find_table(table_id, partition_id);
    std::tuple<MetaDataType, ValueType> *row =
        static_cast<std::tuple<MetaDataType, ValueType> *>(table->search(&key));
    protocol.read(*row, value);
  }

  template <class KeyType, class ValueType>
  void update(std::size_t table_id, std::size_t partition_id,
              const KeyType &key, const ValueType &value) {
    TableType *table = db.find_table(table_id, partition_id);
    std::tuple<MetaDataType, ValueType> *row =
        static_cast<std::tuple<MetaDataType, ValueType> *>(table->search(&key));
  }

  std::size_t add_to_read_set(const RWKeyType &key) {
    readSet.push_back(key);
    return readSet.size() - 1;
  }

  std::size_t add_to_write_set(const RWKeyType &key) {
    writeSet.push_back(key);
    return writeSet.size() - 1;
  }

protected:
  DatabaseType &db;
  ContextType &context;
  RandomType &random;
  ProtocolType &protocol;
  std::vector<RWKeyType> readSet, writeSet;
};

} // namespace scar

