#include <bit>
#include <iostream>

#include <tlx/container/ring_buffer.hpp>
#include <tdc/util/concepts.hpp>

#include "top_k_substrings.hpp"

template<tdc::InputIterator<char> In>
void top_k_compress(In begin, In const end, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k, window_size, sketch_rows, sketch_columns);

    size_t const bits_per_frequent_phrase = std::bit_width(k - 1);
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
                    ++num_frequent;
                    next_phrase = i + longest.length;
                    naive_enc_size += bits_per_frequent_phrase;
                } else {
                    ++num_literal;
                    next_phrase = i + 1;
                    naive_enc_size += bits_per_frequent_phrase + 8;
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

    // debug
    topk.print_debug_info();
    std::cout << "parse"
        << " n=" << (i - window_size + 1)
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << ", naive_enc_size=" << (naive_enc_size/8)
        << std::endl;
}
