#pragma once

#include <cstdint>
#include <iopp/concepts.hpp>

#include <phrase_block_reader.hpp>

constexpr uint64_t LZLIKE_MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'7') << 40 |
    ((uint64_t)'7') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'I') << 16 |
    ((uint64_t)'K') << 8 |
    ((uint64_t)'E');

constexpr bool LZLIKE_DEBUG = false;

template<iopp::BitSource In, std::output_iterator<char> Out>
void lz77like_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != LZLIKE_MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << LZLIKE_MAGIC << ")" << std::endl;
        std::abort();
    }

    std::string dec; // yes, we do it in RAM...

    size_t num_ref = 0;
    size_t num_literal = 0;

    PhraseBlockReader reader(in, true);
    while(in) {
        auto len = reader.read_len();
        if(len > 0) {
            ++num_ref;

            auto const src = reader.read_ref();
            assert(src > 0);

            if constexpr(LZLIKE_DEBUG) std::cout << dec.length() << ": REFERENCE (" << src << ", " << len << ")" << std::endl;            

            auto const i = dec.length();
            assert(i >= src);

            auto j = i - src;
            while(len--) dec.push_back(dec[j++]);
        } else {
            ++num_literal;

            auto const c = reader.read_literal();
            if constexpr(LZLIKE_DEBUG) std::cout << dec.length() << ": LITERAL " << display(c) << std::endl;

            dec.push_back(c);
        }
    }

    // output
    std::copy(dec.begin(), dec.end(), out);
}
