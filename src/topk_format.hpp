#pragma once

#include <cstdint>
#include <iostream>
#include <stdexcept>

#include <tdc/code/universal/binary.hpp>

#include "always_inline.hpp"
#include "vitter87.hpp"

class TopkFormat {
private:
    using Binary = tdc::code::Binary;
    using Universe = tdc::code::Universe;

    Vitter87<uint64_t> huff_phrases;
    Vitter87<uint16_t> huff_literals;

    Universe u_literal;
    Universe u_freq;

    void init() {
        u_literal = Universe(0, 255);
        u_freq = Universe(0, k-1);

        if(huffman_coding) {
            // we cannot encode 0 as of now, so every phrase or literal gets increased by one when encoding...
            huff_phrases = Vitter87<size_t>(k + 1);
            huff_literals = Vitter87<uint16_t>(256);
        }
    }

public:
    uint64_t k;
    uint64_t window_size;
    uint64_t num_sketches;
    uint8_t  sketch_rows;
    uint64_t sketch_columns;
    bool     huffman_coding;

    TopkFormat(uint64_t const _k, uint64_t const _window_size, uint64_t const _num_sketches, uint8_t const _sketch_rows, uint64_t const _sketch_columns, bool const _huffman_coding)
        : k(_k), window_size(_window_size), num_sketches(_num_sketches), sketch_rows(_sketch_rows), sketch_columns(_sketch_columns), huffman_coding(_huffman_coding) {
        
        init();
    }

    template<tdc::code::BitSource In>
    TopkFormat(In& in, uint64_t const expected_magic) {
        uint64_t const magic = Binary::decode(in, Universe::of<uint64_t>());
        if(magic != expected_magic) {
            std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << expected_magic << ")" << std::endl;
            std::abort();
        }

        k = Binary::decode(in, Universe::of<uint64_t>());
        window_size = Binary::decode(in, Universe::of<uint64_t>());
        num_sketches = Binary::decode(in, Universe::of<uint64_t>());
        sketch_rows = Binary::decode(in, Universe::of<uint8_t>());
        sketch_columns = Binary::decode(in, Universe::of<uint64_t>());
        huffman_coding = in.read();

        init();
    }

    template<tdc::code::BitSink Out>
    void encode_header(Out& out, uint64_t const magic) {
        Binary::encode(out, magic, Universe::of<uint64_t>());
        Binary::encode(out, k, Universe::of<uint64_t>());
        Binary::encode(out, window_size, Universe::of<uint64_t>());
        Binary::encode(out, num_sketches, Universe::of<uint64_t>());
        Binary::encode(out, sketch_rows, Universe::of<uint8_t>());
        Binary::encode(out, sketch_columns, Universe::of<uint64_t>());
        out.write(huffman_coding);
    }

    template<tdc::code::BitSink Out>
    void encode_phrase(Out& out, uint64_t const x) {
        if(huffman_coding) {
            auto const code = huff_phrases.encode_and_transmit(x+1);
            huff_phrases.update(x+1);
            out.write(code.word, code.length);
        } else {
            Binary::encode(out, x, u_freq);
        }
    }

    template<tdc::code::BitSource In>
    uint64_t decode_phrase(In& in) {
        if(huffman_coding) {
            auto recv = [&](){ return in.read(); };
            auto const x = huff_phrases.receive_and_decode(recv) - 1;
            huff_phrases.update(x+1);
            return x;
        } else {
            return Binary::decode(in, u_freq);
        }
    }

    template<tdc::code::BitSink Out>
    void encode_literal(Out& out, char const c) {
        if(huffman_coding) {
            uint16_t const x = (uint16_t)((uint8_t)c);
            auto const code = huff_literals.encode_and_transmit(x+1);
            huff_literals.update(x+1);
            out.write(code.word, code.length);
        } else {
            Binary::encode(out, c, u_literal);
        }
    }

    template<tdc::code::BitSource In>
    char decode_literal(In& in) {
        if(huffman_coding) {
            auto recv = [&](){ return in.read(); };
            auto const x = huff_literals.receive_and_decode(recv) - 1;
            huff_literals.update(x+1);
            return (char)x;
        } else {
            return (char)Binary::decode(in, u_literal);
        }
    }
};

