#pragma once

#include <cstdint>
#include <iopp/concepts.hpp>

#include <block_coding.hpp>

namespace lzlike {

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'7') << 40 |
    ((uint64_t)'7') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'I') << 16 |
    ((uint64_t)'K') << 8 |
    ((uint64_t)'E');

constexpr bool DEBUG = false;

constexpr TokenType TOK_LEN = 0;
constexpr TokenType TOK_SRC = 1;
constexpr TokenType TOK_LITERAL = 2;

void setup_encoding(BlockEncodingBase& enc) {
    enc.register_huffman();   // TOK_LEN
    enc.register_binary(SIZE_MAX); // TOK_SRC
    enc.register_huffman();   // TOK_LITERAL
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    std::string s; // yes, we do it in RAM...

    size_t num_ref = 0;
    size_t num_literal = 0;

    BlockDecoder dec(in);
    setup_encoding(dec);
    while(in) {
        auto len = dec.read_uint(TOK_LEN);
        if(len > 0) {
            ++num_ref;

            auto const src = dec.read_uint(TOK_SRC);
            assert(src > 0);

            if constexpr(DEBUG) std::cout << s.length() << ": REFERENCE (" << src << ", " << len << ")" << std::endl;            

            auto const i = s.length();
            assert(i >= src);

            auto j = i - src;
            while(len--) s.push_back(s[j++]);
        } else {
            ++num_literal;

            auto const c = dec.read_char(TOK_LITERAL);
            if constexpr(DEBUG) std::cout << s.length() << ": LITERAL " << display(c) << std::endl;

            s.push_back(c);
        }
    }

    // output
    std::copy(s.begin(), s.end(), out);
}

}
