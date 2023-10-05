#pragma once

#include <algorithm>
#include <bit>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>

template<std::unsigned_integral Frequency>
class CountMin2 {
private:
    static constexpr size_t random_seed_ = 147;

    std::unique_ptr<Frequency[]> table_;
    uintmax_t q1_, q2_;
    size_t num_columns_;
    size_t cmask_;

    size_t h1(uintmax_t const item) const {
        static constexpr uintmax_t p1 = (1ULL << 45) - 229;
        return ((item ^ q1_) % p1) & cmask_;
    }

    size_t h2(uintmax_t const item) const {
        static constexpr uintmax_t p2 = (1ULL << 45) - 193;
        return num_columns_ + ((item ^ q2_) % p2) & cmask_;
    }

public:
    inline CountMin2() : num_columns_(0) {
    }

    inline CountMin2(size_t const columns) {
        auto const cbits = std::bit_width(columns - 1);
        num_columns_ = 1ULL << cbits;
        cmask_ = num_columns_ - 1;

        // initialize frequency table
        table_ = std::make_unique<Frequency[]>(2 * num_columns_);
        for(size_t j = 0; j < 2 * num_columns_; j++) table_[j] = 0;

        // initialize hash functions
        {
            std::mt19937_64 gen(random_seed_);
            q1_ = gen();
            q2_ = gen();
        }
    }

    CountMin2(CountMin2&&) = default;
    CountMin2& operator=(CountMin2&&) = default;
    CountMin2(CountMin2 const&) = delete;
    CountMin2& operator=(CountMin2 const&) = delete;

    template<bool conservative = false>
    inline Frequency increment_and_estimate(uintmax_t const item, Frequency const inc) {
        auto const j1 = h1(item);
        auto const j2 = h2(item);

        auto f1 = table_[j1];
        auto f2 = table_[j2];

        if constexpr(conservative) {
            auto const fmin = std::min(f1, f2);
            if(f1 == fmin) { f1 += inc; table_[j1] = f1; }
            if(f2 == fmin) { f2 += inc; table_[j2] = f2; }
        }

        return std::min(f1, f2);
    }

    template<bool conservative = false>
    inline void increment(uintmax_t const item, Frequency const inc) {
        if constexpr(conservative) {
            auto const j1 = h1(item);
            auto const j2 = h2(item);

            auto const f1 = table_[j1];
            auto const f2 = table_[j2];
            auto const fmin = std::min(f1, f2);

            if(f1 == fmin) { table_[j1] = f1 + inc; }
            if(f2 == fmin) { table_[j2] = f2 + inc; }
        } else {
            table_[h1(item)] += inc;
            table_[h2(item)] += inc;
        }
    }
};
