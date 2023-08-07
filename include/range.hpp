#pragma once

#include <algorithm>
#include <concepts>

#include <code/universe.hpp>

struct Range {
    uintmax_t min, max;

    inline Range() {
        reset();
    }

    inline Range(uintmax_t const min, uintmax_t const max) : min(min), max(max) {
    }

    template<std::unsigned_integral T>
    void contain(T const value) {
        min = std::min(min, uintmax_t(value));
        max = std::max(max, uintmax_t(value));
    }

    void reset() {
        min = UINTMAX_MAX;
        max = 0;
    }

    code::Universe universe() const { return code::Universe(min, max); }
};