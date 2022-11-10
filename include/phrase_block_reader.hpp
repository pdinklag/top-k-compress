#pragma once

#include "phrase.hpp"

#include <iopp/concepts.hpp>

#include <tdc/code/universal/binary.hpp>
#include <tdc/code/universal/elias_delta.hpp>

#include <vector>

template<iopp::BitSource In, std::unsigned_integral Ref = uint32_t>
class PhraseBlockReader {
private:
    In* in_;

    size_t block_size_;
    bool use_len_;
    size_t read_;

    tdc::code::Universe u_refs_;
    tdc::code::Universe u_lits_;
    tdc::code::Universe u_lens_;

    void advance_block() {
        // reset current block
        read_ = 0;

        // load block header
        auto const ref_min = tdc::code::Binary::decode(*in_, tdc::code::Universe::of<Ref>());
        auto const ref_max = tdc::code::Binary::decode(*in_, tdc::code::Universe::of<Ref>());
        u_refs_ = tdc::code::Universe(ref_min, ref_max);

        if(use_len_) {
            auto const len_min = tdc::code::EliasDelta::decode(*in_, tdc::code::Universe::of<Ref>());
            auto const len_max = len_min + tdc::code::EliasDelta::decode(*in_, tdc::code::Universe::of<Ref>());
            u_lens_ = tdc::code::Universe(len_min, len_max);
        }

        auto const lit_min = tdc::code::EliasDelta::decode(*in_, tdc::code::Universe::of<uint8_t>());
        auto const lit_max = lit_min + tdc::code::EliasDelta::decode(*in_, tdc::code::Universe::of<uint8_t>());
        u_lits_ = tdc::code::Universe(lit_min, lit_max);
    }

    void check_underflow() {
        if(read_ >= block_size_) {
            advance_block();
        }
    }

public:
    PhraseBlockReader(In& in, bool const use_len = false) : in_(&in), use_len_(use_len) {
        // load header
        block_size_ = tdc::code::Binary::decode(*in_, tdc::code::Universe::of<uint64_t>());
        
        // make sure we underflow on the very first read
        read_ = block_size_;
    }

    Ref read_ref() {
        check_underflow();
        ++read_;
        return tdc::code::Binary::decode(*in_, u_refs_);
    }

    char read_literal() {
        check_underflow();
        ++read_;
        return (char)tdc::code::Binary::decode(*in_, u_lits_);
    }

    Ref read_len() {
        check_underflow();
        ++read_;
        return (char)tdc::code::Binary::decode(*in_, u_lens_);
    }
};
