//
// Created by Yi Lu on 7/22/18.
//

#ifndef SCAR_TRANSACTION_H
#define SCAR_TRANSACTION_H

#include <vector>

namespace scar {


    template<class Protocol>
    class Transaction {
    public:
        using RWKeyType = typename Protocol::RWKeyType;


    private:
        std::vector<RWKeyType> readSet, writeSet;
    };

}

#endif //SCAR_TRANSACTION_H
