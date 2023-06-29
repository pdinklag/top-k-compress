#pragma once

#include <array>
#include <bit>
#include <concepts>
#include <cstdint>
#include <iterator>
#include <limits>
#include <random>
#include <stdexcept>

#include <mersenne61.hpp>

class RollingKarpRabin {
private:
    static constexpr uint128_t MERSENNE61_SQUARE = (uint128_t)Mersenne61::PRIME * Mersenne61::PRIME;

    inline static uint128_t mult(uint64_t const a, uint64_t const b) {
        return ((uint128_t)a) * b;
    }

    inline static uint64_t modulo(uint128_t const value) {
        // this assumes value < (2^61)^2 = 2^122
        uint128_t const v = value + 1;
        uint64_t const z = ((v >> Mersenne61::EXPONENT) + v) >> Mersenne61::EXPONENT;
        return (value + z) & Mersenne61::PRIME;
    }

    inline static uint128_t square(const uint64_t a) {
        return ((uint128_t)a) * a;
    }

    inline static uint64_t power(uint64_t base, uint64_t exponent) {
        uint64_t result = 1;
        while(exponent > 0) {
            if(exponent & 1ULL) result = modulo(mult(base, result));
            base = modulo(square(base));
            exponent >>= 1;
        }
        return result;
    }

    uint64_t base_;
    std::array<uint128_t, 256> pop_left_precomp_;

public:
    RollingKarpRabin() : base_(0) {
    }

    RollingKarpRabin(uint64_t const window, uint64_t const base) : base_(modulo(base)) {
        auto const max_exponent_exclusive = power(base_, window);
        for(uint64_t c = 0; c < 256; c++) {
            pop_left_precomp_[c] = MERSENNE61_SQUARE - mult(max_exponent_exclusive, c);
        }
    }

    RollingKarpRabin(const RollingKarpRabin&) = default;
    RollingKarpRabin(RollingKarpRabin&&) = default;
    RollingKarpRabin& operator=(const RollingKarpRabin&) = default;
    RollingKarpRabin& operator=(RollingKarpRabin&&) = default;
    
    inline uint64_t roll(uint64_t const fp, uint8_t const pop_left, uint8_t const push_right) {
        const uint128_t shifted_fingerprint = mult(base_, fp);
        const uint128_t pop = pop_left_precomp_[pop_left];
        return modulo(shifted_fingerprint + pop + (uint64_t)push_right);
    }

    inline uint64_t push(uint64_t const fp, uint8_t const push_right) {
        const uint128_t shifted_fingerprint = mult(base_, fp);
        const uint128_t pop = MERSENNE61_SQUARE;
        return modulo(shifted_fingerprint + pop + (uint64_t)push_right);
    }
};
