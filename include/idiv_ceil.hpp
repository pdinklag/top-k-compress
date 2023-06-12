#pragma once

#include <cstddef>

static constexpr size_t idiv_ceil(size_t const a, size_t const b) {
    return ((a + b) - 1ULL) / b;
}
