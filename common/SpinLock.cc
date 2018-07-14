//
// Created by Yi Lu on 7/14/18.
//

#include "spinlock.h"

std::ostream &operator<<(std::ostream &out, const SpinLock &lock) {
    return out << &lock.lock_;
}