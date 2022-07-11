#include <iostream>

#include <tlx/container/ring_buffer.hpp>
#include <tdc/util/concepts.hpp>

#include "top_k_substrings.hpp"

template<tdc::InputIterator<char> In>
void top_k_compress(In begin, In const end, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k, window_size, sketch_rows, sketch_columns);

    size_t i = 0;
    while(begin != end) {
        // read next character
        char const c = *begin++;
        buf.push_back(c);

        // advance
        ++i;
        if(i >= window_size) {
            // count window_size prefixes starting from position (i-window_size)
            auto longest = topk.count_prefixes(buf);

            // TODO: encode phrase

            // advance
            buf.pop_front();
        }
    }

    // topk.print_debug_info();
}
