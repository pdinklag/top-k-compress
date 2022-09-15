#pragma once

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
#include "phrase_block_reader.hpp"
#include "phrase_block_writer.hpp"
#include "topk_format.hpp"
#include "topk_substrings.hpp"

constexpr uint64_t MAGIC_EXH =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'E') << 24 |
    ((uint64_t)'X') << 16 |
    ((uint64_t)'H') << 8 |
    ((uint64_t)'#');

constexpr bool EXH_DEBUG = false;
constexpr bool EXH_PROTOCOL = false;

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_exh(In begin, In const& end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    TopkFormat f(k, window_size, num_sketches, sketch_rows, sketch_columns, false);
    if(!omit_header) f.encode_header(out, MAGIC_EXH);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    TopKSubstrings<> topk(k, num_sketches, sketch_rows, sketch_columns);

    // initialize encoding
    PhraseBlockWriter writer(out, block_size);

    struct NewNode {
        size_t index;
        size_t pos;
    };
    std::list<NewNode> new_nodes;

    TopKSubstrings<>::StringState s[window_size];
    TopKSubstrings<>::StringState match[window_size];
    for(size_t j = 0; j < window_size; j++) {
        s[j] = topk.empty_string();
        match[j] = topk.empty_string();
    }
    size_t longest = 0;

    size_t num_frequent = 0;
    size_t num_literal = 0;

    size_t i = 0;
    size_t next_phrase = 0;

    auto handle = [&](char const c, size_t len) {
        if constexpr(EXH_DEBUG) {
            std::cout << "read next character: " << display(c) << ", i=" << i << ", next_phrase=" << next_phrase << ", longest=" << longest << std::endl;
        }

        // update the w cursors and find the maximum current match
        size_t const num_active_windows = std::min(window_size, i + 1);
        for(size_t j = 0; j < num_active_windows; j++) {
            s[j] = topk.extend(s[j], c);
            if(s[j].frequent) {
                match[j] = s[j];
            }
            if(s[j].new_node) {
                new_nodes.push_back({ s[j].node, i + 1 - s[j].len });
            }
        }

        // test if our buffers are full
        if(i + 1 >= window_size) {
            assert(s[longest].len == window_size);

            // decide whether something must be encoded now
            assert(i + 1 - window_size <= next_phrase); // if this doesn't hold, we missed something
            if(i + 1 - window_size == next_phrase) {
                if constexpr(EXH_DEBUG) std::cout << "- [ENCODE] ";

                // our longest phrase is now exactly w long; encode whatever is possible
                auto phrase_index = match[longest].node;
                auto phrase_len = match[longest].len;

                if(phrase_len >= 1) {
                    // check if the phrase node was only recently created
                    for(auto& recent : new_nodes) {
                        if(recent.index == phrase_index) {
                            // it has - determine the maximum possible length for the phrase
                            size_t const max_len = next_phrase - recent.pos;
                            if(phrase_len > max_len) {
                                // phrase is too long, limit
                                phrase_index = topk.limit(match[longest], max_len);
                                phrase_len = max_len;

                                if constexpr(EXH_DEBUG) {
                                    std::cout << "(LIMITED from index=" << phrase_index << ", length=" << phrase_len << ") ";
                                }
                            }
                            break;
                        }
                    }

                    if constexpr(EXH_DEBUG) {
                        std::cout << "frequent phrase: index=" << phrase_index << ", length=" << phrase_len << std::endl;
                    }
                    if constexpr(EXH_PROTOCOL) {
                        std::cout << "(" << phrase_index << ") / " << phrase_len << std::endl;
                    }

                    assert(phrase_index > 0);
                    writer.write_ref(phrase_index);

                    ++num_frequent;
                    next_phrase += phrase_len;
                } else {
                    auto const x = s[longest].first;

                    if constexpr(EXH_DEBUG) {
                        std::cout << "literal phrase: " << display(x) << std::endl;
                    }
                    if constexpr(EXH_PROTOCOL) {
                        std::cout << "0x" << std::hex << size_t(x) << std::dec << std::endl;
                    }
                    
                    writer.write_ref(0);
                    writer.write_literal(x);

                    ++num_literal;
                    ++next_phrase;
                }
            }

            // advance longest
            s[longest] = topk.empty_string();
            match[longest] = topk.empty_string();
            longest = (longest + 1) % window_size;
        }

        // discard outdated entries in new_nodes
        if(i + 1 >= 2 * window_size) {
            while(!new_nodes.empty() && new_nodes.front().pos < i + 1 - 2 * window_size) {
                new_nodes.pop_front();
            }
        }

        // advance position
        ++i;
    };
    
    while(begin != end) {
        // read next character
        handle(*begin++, window_size);
    }

    // pad trailing zeroes (is this needed?)
    for(size_t x = 0; x < window_size - 1; x++) {
        handle(0, window_size - 1 - x);
    }

    writer.flush();
    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << (i - window_size + 1)
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << std::endl;
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_exh(In in, Out out) {
    using namespace tdc::code;

    // decode header
    TopkFormat f(in, MAGIC_EXH);
    auto const k = f.k;
    auto const window_size = f.window_size;
    auto const num_sketches = f.num_sketches;
    auto const sketch_rows = f.sketch_rows;
    auto const sketch_columns = f.sketch_columns;
    auto const huffman_coding = f.huffman_coding;

    // initialize decompression
    // - frequent substring 0 is reserved to indicate a literal character
    TopKSubstrings<> topk(k, num_sketches, sketch_rows, sketch_columns);

    // initialize decoding
    PhraseBlockReader reader(in);

    TopKSubstrings<>::StringState s[window_size];
    for(size_t j = 0; j < window_size; j++) {
        s[j] = topk.empty_string();
    }
    size_t longest = 0;
    size_t n = 0;

    auto handle = [&](char const c){
        // emit character
        *out++ = c;

        // update the w cursors and find the maximum current match
        size_t const num_active_windows = std::min(window_size, n + 1);
        for(size_t j = 0; j < num_active_windows; j++) {
            s[j] = topk.extend(s[j], c);
        }

        if(n + 1 >= window_size) {
            // advance longest
            assert(s[longest].len == window_size);

            s[longest] = topk.empty_string();
            longest = (longest + 1) % window_size;
        }

        // advance position
        ++n;
    };

    char frequent_string[window_size];
    size_t num_frequent = 0;
    size_t num_literal = 0;

    while(in) {
        auto const p = reader.read_ref();
        if(p) {
            // decode frequent phrase
            ++num_frequent;

            auto const len = topk.get(p, frequent_string);

            if constexpr(EXH_DEBUG) {
                frequent_string[len] = 0;
                std::cout << "- [DECODE] frequent phrase: \"" << frequent_string << "\" (index=" << p << ", length=" << len << ")" << std::endl;
            }
            if constexpr(EXH_PROTOCOL) {
                std::cout << "(" << p << ") / " << len << std::endl;
            }
            
            for(size_t i = 0; i < len; i++) {
                handle(frequent_string[i]);
            }
        } else {
            // decode literal phrase
            ++num_literal;
            char const c = reader.read_literal();
            if constexpr(EXH_DEBUG) {
                std::cout << "- [DECODE] literal phrase: " << display(c) << std::endl;
            }
            if constexpr(EXH_PROTOCOL) {
                std::cout << "0x" << std::hex << size_t(c) << std::dec << std::endl;
            }
            handle(c);
        }
    }

    // debug
    std::cout << "decompress"
        << " k=" << k
        << " w=" << window_size
        << " s=" << num_sketches
        << " c=" << sketch_columns
        << " r=" << sketch_rows
        << " huffman=" << huffman_coding
        << ": n=" << n
        << ", num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << std::endl;
}
