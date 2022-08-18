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
#include "top_k_substrings.hpp"
#include "vitter87.hpp"

constexpr uint64_t MAGIC = 0x54'4F'50'4B'43'4F'4D'50ULL; // spells "TOPKCOMP" in hex
constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;

template<tdc::InputIterator<char> In, tdc::code::BitSink Out, bool huffman_coding>
void top_k_compress(In& begin, In const& end, Out& out, bool const omit_header, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    // encode header
    if(!omit_header) {
        Binary::encode(out, MAGIC, Universe::of<uint64_t>());
        Binary::encode(out, k, Universe::of<size_t>());
        Binary::encode(out, window_size, Universe::of<size_t>());
        Binary::encode(out, num_sketches, Universe::of<size_t>());
        Binary::encode(out, sketch_rows, Universe::of<size_t>());
        Binary::encode(out, sketch_columns, Universe::of<size_t>());
        out.write(huffman_coding);
    }

    Vitter87<size_t> huff_phrases;
    Vitter87<uint16_t> huff_literals;
    
    if constexpr(huffman_coding) {
        // we cannot encode 0 as of now, so every phrase or literal gets increased by one when encoding...
        huff_phrases = Vitter87<size_t>(k + 1);
        huff_literals = Vitter87<uint16_t>(256);
    }

    Universe const u_literal(0, 255);
    Universe const u_freq(0, k-1);

    auto encode_phrase = [&](size_t const x){
        if constexpr(huffman_coding) {
            auto const code = huff_phrases.encode_and_transmit(x+1);
            huff_phrases.update(x+1);
            out.write(code.word, code.length);
        } else {
            Binary::encode(out, x, u_freq);
        }
    };

    auto encode_literal = [&](char const c){
        if constexpr(huffman_coding) {
            uint16_t const x = (uint16_t)((uint8_t)c);
            auto const code = huff_literals.encode_and_transmit(x+1);
            huff_literals.update(x+1);
            out.write(code.word, code.length);
        } else {
            Binary::encode(out, c, u_literal);
        }
    };

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    // - frequent substring k-1 is reserved to indicate the end of file
    TopKSubstrings topk(k-1, window_size, num_sketches, sketch_rows, sketch_columns);

    struct NewNode {
        size_t index;
        size_t pos;
    };
    std::list<NewNode> new_nodes;

    TopKSubstrings::StringState s[window_size];
    TopKSubstrings::StringState match[window_size];
    for(size_t j = 0; j < window_size; j++) {
        s[j] = topk.empty_string();
        match[j] = topk.empty_string();
    }
    size_t longest = 0;

    size_t num_frequent = 0;
    size_t num_literal = 0;
    size_t naive_enc_size = 0;

    size_t i = 0;
    size_t next_phrase = 0;

    auto handle = [&](char const c, size_t len) {
        if constexpr(DEBUG) {
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
                if constexpr(DEBUG) std::cout << "- [ENCODE] ";

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

                                if constexpr(DEBUG) {
                                    std::cout << "(LIMITED from index=" << phrase_index << ", length=" << phrase_len << ") ";
                                }
                            }
                            break;
                        }
                    }

                    if constexpr(DEBUG) {
                        std::cout << "frequent phrase: index=" << phrase_index << ", length=" << phrase_len << std::endl;
                    }
                    if constexpr(PROTOCOL) {
                        std::cout << "(" << phrase_index << ") / " << phrase_len << std::endl;
                    }

                    assert(phrase_index > 0);
                    encode_phrase(phrase_index);

                    ++num_frequent;
                    next_phrase += phrase_len;
                    naive_enc_size += u_freq.entropy();
                } else {
                    auto const x = s[longest].first;

                    if constexpr(DEBUG) {
                        std::cout << "literal phrase: " << display(x) << std::endl;
                    }
                    if constexpr(PROTOCOL) {
                        std::cout << "0x" << std::hex << size_t(x) << std::dec << std::endl;
                    }
                    
                    encode_phrase(0);
                    encode_literal(x);

                    ++num_literal;
                    ++next_phrase;
                    naive_enc_size += u_freq.entropy() + u_literal.entropy();
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

    // encode EOF
    encode_phrase(k-1);

    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << (i - window_size + 1)
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << ", naive_enc_size=" << tdc::idiv_ceil(naive_enc_size, 8)
        << std::endl;
}

template<tdc::InputIterator<char> In, tdc::code::BitSink Out>
void top_k_compress_binary(In begin, In const end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns) {
    top_k_compress<In, Out, false>(begin, end, out, omit_header, k, window_size, num_sketches, sketch_rows, sketch_columns);
}

template<tdc::InputIterator<char> In, tdc::code::BitSink Out>
void top_k_compress_huff(In begin, In const end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns) {
    top_k_compress<In, Out, true>(begin, end, out, omit_header, k, window_size, num_sketches, sketch_rows, sketch_columns);
}

template<tdc::code::BitSource In, std::output_iterator<char> Out>
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
    size_t const num_sketches = Binary::decode(in, Universe::of<size_t>());
    size_t const sketch_rows = Binary::decode(in, Universe::of<size_t>());
    size_t const sketch_columns = Binary::decode(in, Universe::of<size_t>());
    bool const huffman_coding = in.read();

    // initialize
    Vitter87<size_t> huff_phrases;
    Vitter87<uint16_t> huff_literals;
    
    if(huffman_coding) {
        // we cannot encode 0 as of now, so every phrase or literal gets increased by one when encoding...
        huff_phrases = Vitter87<size_t>(k + 1);
        huff_literals = Vitter87<uint16_t>(256);
    }

    Universe const u_literal(0, 255);
    Universe const u_freq(0, k-1);

    auto recv = [&](){ return in.read(); };

    auto decode_phrase = [&](){
        if(huffman_coding) {
            auto const x = huff_phrases.receive_and_decode(recv) - 1;
            huff_phrases.update(x+1);
            return x;
        } else {
            return Binary::decode(in, u_freq);
        }
    };

    auto decode_literal = [&](){
        if(huffman_coding) {
            auto const x = huff_literals.receive_and_decode(recv) - 1;
            huff_literals.update(x+1);
            return (char)x;
        } else {
            return (char)Binary::decode(in, u_literal);
        }
    };

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    // - frequent substring k-1 is reserved to indicate the end of file
    TopKSubstrings topk(k-1, window_size, num_sketches, sketch_rows, sketch_columns);

    TopKSubstrings::StringState s[window_size];
    TopKSubstrings::StringState match[window_size];
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

    while(true) {
        auto const p = decode_phrase();
        if(p == k-1) {
            // EOF
            break;
        }

        if(p) {
            // decode frequent phrase
            ++num_frequent;

            auto const len = topk.get(p, frequent_string);

            if constexpr(DEBUG) {
                frequent_string[len] = 0;
                std::cout << "- [DECODE] frequent phrase: \"" << frequent_string << "\" (index=" << p << ", length=" << len << ")" << std::endl;
            }
            if constexpr(PROTOCOL) {
                std::cout << "(" << p << ") / " << len << std::endl;
            }
            
            for(size_t i = 0; i < len; i++) {
                handle(frequent_string[i]);
            }
        } else {
            // decode literal phrase
            ++num_literal;
            char const c = decode_literal();
            if constexpr(DEBUG) {
                std::cout << "- [DECODE] literal phrase: " << display(c) << std::endl;
            }
            if constexpr(PROTOCOL) {
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
