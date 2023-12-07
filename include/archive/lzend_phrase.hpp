#pragma once

#include <concepts>

template<std::integral Char, std::unsigned_integral Index>
struct LZEndPhrase {
    Index link;
    Index len;
    Char last;
} __attribute__((packed));
