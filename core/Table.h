//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include "common/HashMap.h"

#include "benchmark/tpcc/Schema.h"

namespace scar {

template <class MetaData> class ITable {
public:
  using MetaDataType = MetaData;

  virtual ~ITable() = default;

  virtual std::tuple<MetaData *, void *> search(const void *key) = 0;

  virtual void *search_value(const void *key) = 0;

  virtual MetaDataType &search_metadata(const void *key) = 0;

  virtual void insert(const void *key, const void *value) = 0;

  virtual void update(const void *key, const void *value) = 0;

  virtual std::size_t keyNBytes() = 0;

  virtual std::size_t valueNBytes() = 0;
};

template <std::size_t N, class KeyType, class ValueType, class MetaData>
class Table : public ITable<MetaData> {
public:
  using MetaDataType = MetaData;

  virtual ~Table() override = default;

  Table(std::size_t tableID, std::size_t partitionID)
      : tableID_(tableID), partitionID_(partitionID) {}

  std::tuple<MetaData *, void *> search(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    auto &v = map_[k];
    return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
  }

  void *search_value(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &std::get<1>(map_[k]);
  }

  MetaDataType &search_metadata(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  void insert(const void *key, const void *value) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    bool ok = map_.contains(k);
    CHECK(ok == false);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  void update(const void *key, const void *value) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  std::size_t keyNBytes() override { return sizeof(KeyType); }

  std::size_t valueNBytes() override { return sizeof(ValueType); }

  std::size_t tableID() { return tableID_; }

  std::size_t partitionID() { return partitionID_; }

private:
  HashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;
  std::size_t tableID_;
  std::size_t partitionID_;
};
} // namespace scar
