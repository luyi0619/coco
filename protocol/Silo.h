//
// Created by Yi Lu on 7/18/18.
//

#ifndef SCAR_SILO_H
#define SCAR_SILO_H

#include <atomic>
#include <glog/logging.h>

class Silo {
public:

    using DataType = std::atomic<uint64_t>;

    template<class ValueType>
    ValueType read(std::tuple<DataType, ValueType> &row) {
        DataType &tid = std::get<0>(row);
        ValueType &value = std::get<1>(row);

        ValueType result;
        uint64_t tid_;
        do {
            do {
                tid_ = tid.load();
            } while (isLocked(tid_));
            result = value;
        } while (tid_ != tid.load());
        return result;
    }

    template<class KeyType, class ValueType>
    void update(std::tuple<DataType, ValueType> &row, const ValueType &v) {
        DataType &tid = std::get<0>(row);
        ValueType &value = std::get<1>(row);
        uint64_t tid_ = tid.load();
        CHECK(isLocked(tid_));
        value = v;
    }

private:

    bool isLocked(uint64_t value) {
        return value & LOCK_BIT_MASK;
    }

    uint64_t lock(std::atomic<uint64_t> &a) {
        uint64_t oldValue, newValue;
        do {
            do {
                oldValue = a.load();
            } while (isLocked(oldValue));
            newValue = oldValue | LOCK_BIT_MASK;
        } while (!a.compare_exchange_weak(oldValue, newValue));
        CHECK(isLocked(oldValue) == false);
        return oldValue;
    }

    uint64_t lock(std::atomic<uint64_t> &a, bool &success) {
        uint64_t oldValue = a.load();

        if (isLocked(oldValue)) {
            success = false;
        } else {
            uint64_t newValue = oldValue | LOCK_BIT_MASK;
            success = a.compare_exchange_strong(oldValue, newValue);
        }
        return oldValue;
    }


    static constexpr uint64_t LOCK_BIT_MASK = 0x1ull << 63;
};

#endif //SCAR_SILO_H
