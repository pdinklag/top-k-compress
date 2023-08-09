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

    std::unique_ptr<Frequency[]> row1_, row2_;
    uintmax_t q1_, q2_;
    size_t num_columns_;
    size_t cmask_;


    size_t h1(uintmax_t const item) const {
        static constexpr uintmax_t p1 = (1ULL << 45) - 229;
        return ((item ^ q1_) % p1) & cmask_;
    }

    size_t h2(uintmax_t const item) const {
        static constexpr uintmax_t p2 = (1ULL << 45) - 193;
        return ((item ^ q2_) % p2) & cmask_;
    }

public:
    inline CountMin2() : num_columns_(0) {
    }

    inline CountMin2(size_t const columns) {
        auto const cbits = std::bit_width(columns - 1);
        num_columns_ = 1ULL << cbits;
        cmask_ = num_columns_ - 1;

        // initialize frequency table
        row1_ = std::make_unique<Frequency[]>(num_columns_);
        for(size_t j = 0; j < num_columns_; j++) row1_[j] = 0;

        row2_ = std::make_unique<Frequency[]>(num_columns_);
        for(size_t j = 0; j < num_columns_; j++) row2_[j] = 0;

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

    inline Frequency increment_and_estimate(uintmax_t const item, Frequency const inc) {
        auto const j1 = h1(item);
        auto const j2 = h2(item);

        row1_[j1] += inc;
        row2_[j2] += inc;

        return std::min(row1_[j1], row2_[j2]);
    }

    inline void increment(uintmax_t const item, Frequency const inc) {
        row1_[h1(item)] += inc;
        row2_[h2(item)] += inc;
    }
};
