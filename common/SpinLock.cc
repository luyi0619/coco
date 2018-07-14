//
// Created by Yi Lu on 7/14/18.
//

#include "SpinLock.h"

namespace scar {
    std::ostream &operator<<(std::ostream &out, const SpinLock &lock) {
        return out << &lock.lock_;
    }
}