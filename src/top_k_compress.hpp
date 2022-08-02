#include <bit>
#include <cstdint>
#include <cstdlib>
#include <iostream>

#include <tlx/container/ring_buffer.hpp>

#include <tdc/code/universal/binary.hpp>
#include <tdc/util/concepts.hpp>
#include <tdc/util/math.hpp>

#include "top_k_substrings.hpp"
#include "vitter87.hpp"

constexpr uint64_t MAGIC = 0x54'4F'50'4B'43'4F'4D'50ULL; // spells "TOPKCOMP" in hex
constexpr bool DEBUG = false;

template<tdc::InputIterator<char> In, tdc::io::BitSink Out, bool huffman_coding>
void top_k_compress(In& begin, In const& end, Out& out, bool const omit_header, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    using namespace tdc::code;

    // encode header
    if(!omit_header) {
        Binary::encode(out, MAGIC, Universe::of<uint64_t>());
        Binary::encode(out, k, Universe::of<size_t>());
        Binary::encode(out, window_size, Universe::of<size_t>());
        Binary::encode(out, sketch_rows, Universe::of<size_t>());
        Binary::encode(out, sketch_columns, Universe::of<size_t>());
    }

    // initialize compression
    // notes on top-k:
    // - frequent substring 0 is reserved to indicate a literal character
    // - frequent substring k-1 is reserved to indicate the end of file
    tlx::RingBuffer<char> buf(window_size);
    TopKSubstrings topk(k-1, window_size, sketch_rows, sketch_columns);

    Vitter87<size_t> dhuff;
    
    if constexpr(huffman_coding) {
        dhuff = Vitter87<size_t>(k + 1); // we cannot encode 0 as of now, so every phrase gets increased by one when encoding...
    }

    /*
    size_t phrase_hist[k] = { 0 };
    size_t phrase_bits[k] = { 0 };
    */

    Universe const u_literal(0, 255);
    Universe const u_freq(0, k-1);

    auto encode_phrase = [&](size_t const x){
        if constexpr(huffman_coding) {
            auto const code = dhuff.encode_and_transmit(x+1);
            dhuff.update(x+1);
            out.write(code.word, code.length);
        } else {
            Binary::encode(out, x, u_freq);
        }
        // ++phrase_hist[x];
        // phrase_bits[x] += code.length;
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
            //
            if constexpr(DEBUG) {
                std::cout << "- window: \"";
                for(size_t j = 0; j < len; j++) {
                    std::cout << buf[j];
                }
                std::cout << "\"" << std::endl;
            }

            // count window_size prefixes starting from position (i-window_size)
            auto longest = topk.count_prefixes_and_match(buf, len, std::min(len, i + 1 - window_size));

            // encode phrase
            if(i >= next_phrase) {
                if(longest.length >= 1) {
                    if constexpr(DEBUG) {
                        std::cout << "- [ENCODE] frequent phrase: \"";
                        for(size_t j = 0; j < longest.length; j++) {
                            std::cout << buf[j];
                        }
                        std::cout << "\" (length=" << longest.length << ", index=" << longest.index << ")" << std::endl;
                    }

                    encode_phrase(longest.index);

                    ++num_frequent;
                    next_phrase = i + longest.length;
                    naive_enc_size += u_freq.entropy();
                } else {
                    if constexpr(DEBUG) {
                        std::cout << "- [ENCODE] literal phrase: '" << buf[0] << "'";
                        if(longest.length == 1) std::cout << " (frequent phrase of length=1, index=" << longest.index << ")";
                        std::cout << std::endl;
                    }
                    
                    encode_phrase(0);
                    Binary::encode(out, buf[0], u_literal);

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

    // debug
    /*
    std::cout << "phrase distribution:" << std::endl;
    std::cout << "phrase\tfreq\tbits\tavg\tbalance" << std::endl;
    for(size_t i = 0; i < k; i++) {
        std::cout
            << i
            << "\t" << phrase_hist[i]
            << "\t" << phrase_bits[i]
            << "\t" << ((double)phrase_bits[i] / (double)phrase_hist[i])
            << "\t" << (((double)phrase_bits[i] / (double)phrase_hist[i]) - u_freq.entropy())
            << std::endl;
    }
    */

    topk.print_debug_info();
    std::cout << "parse"
        << " n=" << (i - window_size + 1)
        << ": num_frequent=" << num_frequent
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_frequent + num_literal)
        << ", naive_enc_size=" << tdc::idiv_ceil(naive_enc_size, 8)
        << std::endl;
}

template<tdc::InputIterator<char> In, tdc::io::BitSink Out>
void top_k_compress_binary(In begin, In const end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    top_k_compress<In, Out, false>(begin, end, out, omit_header, k, window_size, sketch_rows, sketch_columns);
}

template<tdc::InputIterator<char> In, tdc::io::BitSink Out>
void top_k_compress_huff(In begin, In const end, Out out, bool const omit_header, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns) {
    top_k_compress<In, Out, true>(begin, end, out, omit_header, k, window_size, sketch_rows, sketch_columns);
}

template<tdc::io::BitSource In, std::output_iterator<char> Out>
void top_k_decompress_binary(In in, Out out) {
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
    size_t i = 0;

    char frequent_string[window_size + 1]; // +1 for debugging

    auto handle = [&](char const c){
        *out++ = c;
        buf.push_back(c);

        ++i;

        // TODO: count prefixes - how exactly ?

        if(buf.size() == buf.max_size()) buf.pop_front();
    };

    while(true) {
        auto const p = Binary::decode(in, u_freq);
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
                std::cout << "- [DECODE] frequent phrase: \"" << frequent_string << "\" (length=" << len << ", index=" << p << ")" << std::endl;
            }
            
            for(size_t i = 0; i < len; i++) {
                handle(frequent_string[i]);
            }
        } else {
            // decode literal phrase
            ++num_literal;
            char const c = Binary::decode(in, u_literal);
            if constexpr(DEBUG) {
                std::cout << "- [DECODE] literal phrase: '" << c << "'" << std::endl;
            }
            handle(c);
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
