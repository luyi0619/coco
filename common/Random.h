//
// Created by Yi Lu on 7/14/18.
//

#ifndef SCAR_RANDOM_H
#define SCAR_RANDOM_H

namespace scar {

    class Random {
    public:

        Random(uint64_t seed = 0) {
            init_seed(seed);
        }

        void init_seed(uint64_t seed) {
            seed_ = (seed ^ 0x5DEECE66DULL) & ((1ULL << 48) - 1);
        }

        void set_seed(uint64_t seed) {
            seed_ = seed;
        }

        uint64_t get_seed() {
            return seed_;
        }

        uint64_t next() {
            return ((uint64_t) next(32) << 32) + next(32);
        }

        uint64_t next(unsigned int bits) {
            seed_ = (seed_ * 0x5DEECE66DULL + 0xBULL) & ((1ULL << 48) - 1);
            return (seed_ >> (48 - bits));
        }

        /* [0.0, 1.0) */
        double next_double() {
            return (((uint64_t) next(26) << 27) + next(27)) / (double) (1ULL << 53);
        }

        uint64_t uniform_dist(uint64_t a, uint64_t b) {
            if (a == b) return a;
            return next() % (b - a + 1) + a;
        }

    private:
        uint64_t seed_;
    };
}


#endif //SCAR_RANDOM_H
