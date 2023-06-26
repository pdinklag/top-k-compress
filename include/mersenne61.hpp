#pragma once

#include <cassert>
#include <cstdint>
#include <uint128.hpp>

class Mersenne61 {
public:
    static constexpr uint64_t EXPONENT = 61;
    static constexpr uint64_t PRIME = (uint64_t(1) << 61) - 1;

    inline static uint64_t mod(uint128_t const value) {
        assert(value <= (uint128_t(1) << (2 * EXPONENT)) - 1);

        uint128_t const z = (value + 1) >> EXPONENT;
        return (value + z) & PRIME;
    }

    inline static uint64_t mulmod(uint64_t const a, uint64_t const b) {
        return mod(uint128_t(a) * uint128_t(b));
    }
};
