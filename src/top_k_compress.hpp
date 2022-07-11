#include <iostream>

#include <tlx/container/ring_buffer.hpp>
#include <tdc/util/concepts.hpp>

#include "top_k_substrings.hpp"

template<tdc::InputIterator<char> In>
void top_k_compress(In begin, In const end, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k, window_size, sketch_rows, sketch_columns);

    size_t i = 0;
    auto handle = [&](char const c, size_t len){
        buf.push_back(c);

        // advance
        ++i;
        if(i >= window_size) {
            // count window_size prefixes starting from position (i-window_size)
            auto longest = topk.count_prefixes(buf, len);

            // TODO: encode phrase

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
    // topk.print_debug_info();
}
