#include <iopp/concepts.hpp>

#pragma once

template<std::output_iterator<char> Out>
void write_uint(Out& out, uint64_t const x, size_t const num_bytes) {
    static_assert(std::endian::native == std::endian::little);
    assert(num_bytes <= 8);
    char const* s = (char const*)&x;
    for(size_t i = 0; i < num_bytes; i++) {
        *out++ = s[i];
    }
}

template<iopp::InputIterator<char> In>
uint64_t read_uint(In& in, size_t const num_bytes) {
    assert(num_bytes <= 8);
    uint64_t x = 0;
    char* s = (char*)&x;
    for(size_t i = 0; i < num_bytes; i++) {
        s[i] = *in++;
    }
    return x;
}
