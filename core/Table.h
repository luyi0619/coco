//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include "common/HashMap.h"

namespace scar {

template <class MetaData> class ITable {
public:
  using MetaDataType = MetaData;

  virtual void *search(const void *key) = 0;

  virtual MetaDataType &searchMetaData(const void *key) = 0;

  virtual void insert(const void *key, const void *value) = 0;

  virtual std::size_t keyNBytes() = 0;

  virtual std::size_t valueNBytes() = 0;
};

template <std::size_t N, class KeyType, class ValueType, class MetaData>
class Table : public ITable<MetaData> {
public:
  using MetaDataType = MetaData;

  Table(std::size_t tableID) : tableID_(tableID) {}

  void *search(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return &map_[k];
  }

  MetaDataType &searchMetaData(const void *key) override {
    const auto &k = *static_cast<const KeyType *>(key);
    return std::get<0>(map_[k]);
  }

  void insert(const void *key, const void *value) override {
    const auto &k = *static_cast<const KeyType *>(key);
    const auto &v = *static_cast<const ValueType *>(value);
    auto &row = map_[k];
    std::get<1>(row) = v;
  }

  std::size_t keyNBytes() override { return sizeof(KeyType); }

  std::size_t valueNBytes() override { return sizeof(ValueType); }

private:
  HashMap<N, KeyType, std::tuple<MetaDataType, ValueType>> map_;

  const std::size_t tableID_;
};
} // namespace scar

