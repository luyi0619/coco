//
// Created by Yi Lu on 7/18/18.
//

#ifndef SCAR_TABLE_H
#define SCAR_TABLE_H

#include "common/HashMap.h"

namespace scar {
    class ITable {
    public:
        virtual void *search(void *key) = 0;

        virtual void insert(void *key, void *value) = 0;

        virtual std::size_t keyNBytes() = 0;

        virtual std::size_t valueNBytes() = 0;
    };


    template<std::size_t N, class KeyType, class ValueType, class Protocol>
    class Table : public ITable {
    public:
        using DataType = typename Protocol::DataType;

        Table(std::size_t tableID) : tableID_(tableID) {}

        void *search(void *key) override {
            const auto &k = *static_cast<KeyType *>(key);
            return &map_[k];
        }

        void insert(void *key, void *value) override {
            const auto &k = *static_cast<KeyType *>(key);
            const auto &v = *static_cast<ValueType *>(value);
            auto &row = map_[k];
            std::get<1>(row) = v;
        }

        std::size_t keyNBytes() override {
            return sizeof(KeyType);
        }

        std::size_t valueNBytes() override {
            return sizeof(ValueType);
        }

    private:
        HashMap<N, KeyType, std::tuple<DataType, ValueType>> map_;

        const std::size_t tableID_;
    };
}


#endif //SCAR_TABLE_H
