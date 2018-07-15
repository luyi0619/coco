//
// Created by Yi Lu on 7/14/18.
//

#ifndef SCAR_YCSBRANDOM_H
#define SCAR_YCSBRANDOM_H

#include <string>
#include <vector>

#include "common/Random.h"

namespace scar {

    class YCSBRandom : public Random {
    public:
        using Random::Random;

        std::string rand_str(std::size_t length) {
            auto &characters_ = characters();
            auto characters_len = characters_.length();
            std::string result;
            for (auto i = 0; i < length; i++) {
                int k = uniform_dist(0, characters_len - 1);
                result += characters_[k];
            }
            return result;
        }

    private:

        static const std::string &characters() {
            static std::string characters_ = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            return characters_;
        };

    };
}

#endif //SCAR_YCSBRANDOM_H
