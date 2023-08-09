#pragma once

#include <iopp/concepts.hpp>

#include <code/binary.hpp>
#include <code/huffman.hpp>

#include <vector>

template<iopp::BitSource In, std::unsigned_integral Ref = uint32_t, std::unsigned_integral Len = uint32_t>
class PhraseBlockReader {
private:
    static constexpr bool DEBUG = false;

    using Char = uint8_t;

    In* in_;

    size_t block_size_;
    bool use_len_;
    size_t read_;

    code::Universe u_refs_;
    code::HuffmanTree<Char> huff_lits_;
    code::HuffmanTree<Len> huff_lens_;

    void advance_block() {
        if constexpr(DEBUG) std::cout << "advance block" << std::endl;

        // reset current block
        read_ = 0;

        // load block header
        auto const ref_min = code::Binary::decode(*in_, code::Universe::of<Ref>());
        auto const ref_max = code::Binary::decode(*in_, code::Universe::of<Ref>());
        u_refs_ = code::Universe(ref_min, ref_max);

        if(use_len_) {
            huff_lens_ = decltype(huff_lens_)(*in_);
        }

        huff_lits_ = decltype(huff_lits_)(*in_);
    }

    void check_underflow() {
        if(read_ >= block_size_) {
            advance_block();
        }
    }

public:
    PhraseBlockReader(In& in, bool const use_len = false) : in_(&in), use_len_(use_len) {
        // load header
        block_size_ = code::Binary::decode(*in_, code::Universe::of<uint64_t>());
        
        // make sure we underflow on the very first read
        read_ = block_size_;
    }

    Ref read_ref() {
        check_underflow();
        ++read_;
        return code::Binary::decode(*in_, u_refs_);
    }

    char read_literal() {
        check_underflow();
        ++read_;
        return (char)code::Huffman::decode(*in_, huff_lits_.root());
    }

    Ref read_len() {
        check_underflow();
        ++read_;
        return (Ref)code::Huffman::decode(*in_, huff_lens_.root());
    }
};
