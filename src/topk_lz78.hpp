#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <list>

#include <pm/malloc_counter.hpp>

#include <tdc/code/universal/binary.hpp>
#include <tdc/util/concepts.hpp>
#include <tdc/util/math.hpp>

#include "display.hpp"
#include "topk_format.hpp"
#include "topk_substrings.hpp"

constexpr uint64_t MAGIC_LZ78 =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'Z') << 16 |
    ((uint64_t)'7') << 8 |
    ((uint64_t)'8');

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_lz78(In begin, In const& end, Out out, bool const omit_header, size_t const k, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, bool const huffman_coding) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    TopkFormat f(k, 0 /* indicator for LZ78 compression :-) */, num_sketches, sketch_rows, sketch_columns, huffman_coding);
    if(!omit_header) f.encode_header(out, MAGIC_LZ78);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    // - frequent substring k-1 is reserved to indicate the end of file
    TopKSubstrings<> topk(k-1, num_sketches, sketch_rows, sketch_columns);
    size_t n = 0;
    size_t num_phrases = 0;
    size_t longest = 0;

    TopKSubstrings<>::StringState s = topk.empty_string();
    auto handle = [&](char const c) {
        auto next = topk.extend(s, c);
        if(!next.frequent) {
            longest = std::max(longest, size_t(next.len));
            f.encode_phrase(out, s.node);
            f.encode_literal(out, c);
            s = topk.empty_string();
            ++num_phrases;
        } else {
            s = next;
        }
    };

    while(begin != end) {
        // read next character
        ++n;
        handle(*begin++);
    }

    // encode EOF marker
    f.encode_phrase(out, k-1);

    // encode final phrase, if any
    if(s.len > 0) {
        f.encode_phrase(out, s.node);
        ++num_phrases;
    }

    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << n
        << " -> num_phrases=" << num_phrases
        << ", longest=" << longest
        << std::endl;
}
