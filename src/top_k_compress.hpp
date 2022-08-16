#include <bit>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include <tlx/container/ring_buffer.hpp>

#include <pm/malloc_counter.hpp>

#include <tdc/code/universal/binary.hpp>
#include <tdc/util/concepts.hpp>
#include <tdc/util/math.hpp>

#include "display.hpp"
#include "top_k_substrings.hpp"
#include "vitter87.hpp"

constexpr uint64_t MAGIC = 0x54'4F'50'4B'43'4F'4D'50ULL; // spells "TOPKCOMP" in hex
constexpr bool DEBUG = true;

template<tdc::InputIterator<char> In, tdc::io::BitSink Out, bool huffman_coding>
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

    // initialize compression
    // notes on top-k:
    // - frequent substring 0 is reserved to indicate a literal character
    // - frequent substring k-1 is reserved to indicate the end of file
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k-1, window_size, num_sketches, sketch_rows, sketch_columns);

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

    size_t num_frequent = 0;
    size_t num_literal = 0;
    size_t naive_enc_size = 0;

    size_t i = 0;
    size_t next_phrase = 0;
    auto handle = [&](char const c, size_t len) {
        if constexpr(DEBUG) {
            std::cout << "read next character: '" << c << "'" << std::endl;
        }

        buf.push_back(c);
        if(buf.size() == buf.max_size()) {
            assert(i + 1 >= window_size);

            //
            if constexpr(DEBUG) {
                std::cout << "- window: \"";
                for(size_t j = 0; j < len; j++) {
                    std::cout << buf[j];
                }
                std::cout << "\"" << std::endl;
            }

            // count window_size prefixes starting from position (i-window_size)
            auto longest = topk.empty_string();
            {
                auto s = topk.empty_string();
                size_t const max_match_len = std::min(len, i + 1 - window_size);

                for(size_t j = 0; j < len; j++) {
                    s = topk.extend(s, buf[j]);

                    if(j + 1 < max_match_len && s.frequent) {
                        longest = s;
                    }
                }
            }

            // encode phrase
            if(i >= next_phrase) {
                if(longest.len >= 1) {
                    if constexpr(DEBUG) {
                        std::cout << "- [ENCODE] frequent phrase: \"";
                        for(size_t j = 0; j < longest.len; j++) {
                            std::cout << buf[j];
                        }
                        std::cout << "\" (length=" << longest.len << ", index=" << longest.node << ")" << std::endl;
                    }

                    encode_phrase(longest.node);

                    ++num_frequent;
                    next_phrase = i + longest.len;
                    naive_enc_size += u_freq.entropy();
                } else {
                    if constexpr(DEBUG) {
                        std::cout << "- [ENCODE] literal phrase: " << display(buf[0]);
                        if(longest.len == 1) std::cout << " (frequent phrase of length=1, index=" << longest.node << ")";
                        std::cout << std::endl;
                    }
                    
                    encode_phrase(0);
                    encode_literal(buf[0]);

                    ++num_literal;
                    next_phrase = i + 1;
                    naive_enc_size += u_freq.entropy() + u_literal.entropy();
                }
            }

            // advance
            buf.pop_front();
        }
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

template<tdc::InputIterator<char> In, tdc::io::BitSink Out>
void top_k_compress_binary(In begin, In const end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns) {
    top_k_compress<In, Out, false>(begin, end, out, omit_header, k, window_size, num_sketches, sketch_rows, sketch_columns);
}

template<tdc::InputIterator<char> In, tdc::io::BitSink Out>
void top_k_compress_huff(In begin, In const end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns) {
    top_k_compress<In, Out, true>(begin, end, out, omit_header, k, window_size, num_sketches, sketch_rows, sketch_columns);
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
    size_t const num_sketches = Binary::decode(in, Universe::of<size_t>());
    size_t const sketch_rows = Binary::decode(in, Universe::of<size_t>());
    size_t const sketch_columns = Binary::decode(in, Universe::of<size_t>());
    bool const huffman_coding = in.read_bit();

    // initialize
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k-1, window_size, num_sketches, sketch_rows, sketch_columns);

    Vitter87<size_t> huff_phrases;
    Vitter87<uint16_t> huff_literals;
    
    if(huffman_coding) {
        // we cannot encode 0 as of now, so every phrase or literal gets increased by one when encoding...
        huff_phrases = Vitter87<size_t>(k + 1);
        huff_literals = Vitter87<uint16_t>(256);
    }

    Universe const u_literal(0, 255);
    Universe const u_freq(0, k-1);

    auto recv = [&](){ return in.read_bit(); };

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

    auto s = topk.empty_string();
    size_t n = 0;

    auto handle = [&](char const c){
        *out++ = c;
        buf.push_back(c);

        ++n;

        s = topk.extend(s, c);
        if(buf.size() == buf.max_size()) {
            // our buffer is full, pop the first character, then count all prefixes of the suffix
            buf.pop_front();

            assert(s.len == window_size);
            s = topk.empty_string();
            assert(buf.size() == window_size - 1);
            for(size_t j = 0; j < window_size - 1; j++) {
                s = topk.extend(s, buf[j]);
            }
        }
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
