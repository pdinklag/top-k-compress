#pragma once

#include <cstdint>

struct Phrase {
    uint64_t number;
    bool     is_literal;

    inline Phrase(char const c) : number(c), is_literal(true) {
    }

    inline Phrase(uint64_t const x) : number(x), is_literal(false) {
    }
};
