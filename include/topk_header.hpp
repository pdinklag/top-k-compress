#pragma once

#include <cstdint>
#include <iostream>
#include <stdexcept>

#include <code/binary.hpp>

#include "always_inline.hpp"
#include "vitter87.hpp"

class TopkHeader {
private:
    using Binary = code::Binary;
    using Universe = code::Universe;

public:
    uint64_t k;
    uint64_t window_size;
    uint64_t num_sketches;
    uint8_t  sketch_rows;
    uint64_t sketch_columns;

    TopkHeader(uint64_t const _k, uint64_t const _window_size, uint64_t const _num_sketches, uint8_t const _sketch_rows, uint64_t const _sketch_columns)
        : k(_k), window_size(_window_size), num_sketches(_num_sketches), sketch_rows(_sketch_rows), sketch_columns(_sketch_columns) {
    }

    template<code::BitSource In>
    TopkHeader(In& in, uint64_t const expected_magic) {
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
    }

    template<code::BitSink Out>
    void encode(Out& out, uint64_t const magic) {
        Binary::encode(out, magic, Universe::of<uint64_t>());
        Binary::encode(out, k, Universe::of<uint64_t>());
        Binary::encode(out, window_size, Universe::of<uint64_t>());
        Binary::encode(out, num_sketches, Universe::of<uint64_t>());
        Binary::encode(out, sketch_rows, Universe::of<uint8_t>());
        Binary::encode(out, sketch_columns, Universe::of<uint64_t>());
    }
};

