#include <bit>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include <tlx/container/ring_buffer.hpp>

#include <tdc/code/universal/binary.hpp>
#include <tdc/util/concepts.hpp>
#include <tdc/util/math.hpp>

#include "top_k_substrings.hpp"

constexpr uint64_t MAGIC = 0x54'4F'50'4B'43'4F'4D'50ULL; // spells "TOPKCOMP" in hex

template<tdc::InputIterator<char> In, tdc::io::BitSink Out>
void top_k_compress(In begin, In const end, Out out, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    using namespace tdc::code;

    // encode header
    Binary::encode(out, MAGIC, Universe::of<uint64_t>());
    Binary::encode(out, k, Universe::of<size_t>());
    Binary::encode(out, window_size, Universe::of<size_t>());
    Binary::encode(out, sketch_rows, Universe::of<size_t>());
    Binary::encode(out, sketch_columns, Universe::of<size_t>());

    // initialize compression
    // notes on top-k:
    // - frequent substring 0 is reserved to indicate a literal character
    // - frequent substring k-1 is reserved to indicate the end of file
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k-1, window_size, sketch_rows, sketch_columns);

    Universe const u_literal(0, 255);
    Universe const u_freq(0, k-1);

    size_t num_frequent = 0;
    size_t num_literal = 0;
    size_t naive_enc_size = 0;

    size_t i = 0;
    size_t next_phrase = 0;
    auto handle = [&](char const c, size_t len){
        buf.push_back(c);

        // advance
        ++i;
        if(i >= window_size) {
            // count window_size prefixes starting from position (i-window_size)
            auto longest = topk.count_prefixes(buf, len);

            // encode phrase
            if(i >= next_phrase) {
                if(longest.length > 1) {
                    Binary::encode(out, longest.index, u_freq);

                    ++num_frequent;
                    next_phrase = i + longest.length;
                    naive_enc_size += u_freq.entropy();
                } else {
                    Binary::encode(out, 0, u_freq);
                    Binary::encode(out, c, u_literal);

                    ++num_literal;
                    next_phrase = i + 1;
                    naive_enc_size += u_freq.entropy() + u_literal.entropy();
                }
            }

            // advance
            buf.pop_front();
        }
    };
    
    while(begin != end) {
        // read next character
        handle(*begin++, window_size);
    }

    // pad trailing zeroes
    for(size_t x = 0; x < window_size - 1; x++) {
        handle(0, window_size - 1 - x);
    }

    // encode EOF
    Binary::encode(out, k-1, u_freq);

    // debug
    topk.print_debug_info();
    std::cout << "parse"
        << " n=" << (i - window_size + 1)
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << ", naive_enc_size=" << tdc::idiv_ceil(naive_enc_size, 8)
        << std::endl;
}

template<tdc::io::BitSource In, std::output_iterator<char> Out>
void top_k_decompress(In in, Out out) {
    using namespace tdc::code;

    // decode header
    uint64_t const magic = Binary::decode(in, Universe::of<uint64_t>());
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << magic << std::endl;
        return;
    }

    size_t const k = Binary::decode(in, Universe::of<size_t>());
    size_t const window_size = Binary::decode(in, Universe::of<size_t>());
    size_t const sketch_rows = Binary::decode(in, Universe::of<size_t>());
    size_t const sketch_columns = Binary::decode(in, Universe::of<size_t>());

    // initialize
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k-1, window_size, sketch_rows, sketch_columns);

    Universe const u_literal(0, 255);
    Universe const u_freq(0, k-1);

    size_t num_frequent = 0;
    size_t num_literal = 0;

    char frequent_string[window_size];

    while(true) {
        auto const p = Binary::decode(in, u_freq);
        if(p == k-1) {
            // EOF
            break;
        }
        if(p) {
            // decode frequent phrase
            ++num_frequent;

            // auto const len = topk.get(p, frequent_string);
            // for(size_t i = 0; i < len; i++) {
                // *out++ = frequent_string[i];
                // TODO: update top-k
            // }
        } else {
            // decode literal phrase
            ++num_literal;
            char const c = Binary::decode(in, u_literal);
            // *out++ = c;
            // TODO: update top-k
        }
    }

    // debug
    std::cout << "decompress"
        << " k=" << k
        << " w=" << window_size
        << " c=" << sketch_columns
        << " r=" << sketch_rows
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << std::endl;
}
