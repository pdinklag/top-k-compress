#pragma once

#include <algorithm>
#include <limits>
#include <vector>

#include <iostream>
#include <unordered_set>

#include <iopp/concepts.hpp>

#include <tdc/code/entropical/huffman.hpp>
#include <tdc/code/universal/binary.hpp>
#include <tdc/code/universal/elias_delta.hpp>

template<iopp::BitSink Out, std::unsigned_integral Ref = uint32_t, std::unsigned_integral Len = uint32_t>
class PhraseBlockWriter {
private:
    static constexpr bool DEBUG = false;

    using Char = uint8_t;
    using ItemType = uint8_t;

    static constexpr ItemType TYPE_REF = 0;
    static constexpr ItemType TYPE_LIT = 1;
    static constexpr ItemType TYPE_LEN = 2;

    Out* out_;
    
    size_t block_size_;
    bool use_len_;
    std::vector<Ref> cur_refs_;
    std::vector<Len> cur_lens_;
    std::vector<Char> cur_literals_;
    std::vector<ItemType> cur_block_;

    Ref ref_min_, ref_max_;

    void flush_block() {
        if constexpr(DEBUG) std::cout << "write block of size " << cur_block_.size() << ": refs=" << cur_refs_.size() << ", lens=" << cur_lens_.size() << ", literals=" << cur_literals_.size() << std::endl;

        // encode block header
        tdc::code::Binary::encode(*out_, ref_min_, tdc::code::Universe::of<Ref>());
        tdc::code::Binary::encode(*out_, ref_max_, tdc::code::Universe::of<Ref>());
        tdc::code::Universe u_refs(ref_min_, ref_max_);

        tdc::code::HuffmanTree<Len> huff_len_tree;    
        if(use_len_) {
            huff_len_tree = tdc::code::HuffmanTree<Len>(cur_lens_.begin(), cur_lens_.end());
            huff_len_tree.encode(*out_);
        }
        auto const huff_len = huff_len_tree.table();
        huff_len_tree = decltype(huff_len_tree)();

        tdc::code::HuffmanTree<Char> huff_lit_tree(cur_literals_.begin(), cur_literals_.end());
        huff_lit_tree.encode(*out_);
        auto const huff_lit = huff_lit_tree.table();
        huff_lit_tree = decltype(huff_lit_tree)();

        // encode current block
        size_t next_lit = 0;
        size_t next_ref = 0;
        size_t next_len = 0;
        for(auto const type : cur_block_) {
            switch(type) {
                case TYPE_REF:
                    tdc::code::Binary::encode(*out_, cur_refs_[next_ref++], u_refs);
                    break;
                
                case TYPE_LIT:
                    tdc::code::Huffman::encode(*out_, cur_literals_[next_lit++], huff_lit);
                    break;

                case TYPE_LEN:
                    tdc::code::Huffman::encode(*out_, cur_lens_[next_len++], huff_len);
                    break;
            }
        }

        // clear current block
        cur_block_.clear();
        cur_refs_.clear();
        cur_literals_.clear();
        cur_lens_.clear();
        reset_universe();
    }

    void check_overflow() {
        if(cur_block_.size() >= block_size_) {
            flush_block();
        }
    }

    void reset_universe() {
        ref_min_ = std::numeric_limits<Ref>::max();
        ref_max_ = 0;
    }

public:
    PhraseBlockWriter(Out& out, size_t const block_size, bool const use_len = false) : out_(&out), block_size_(block_size), use_len_(use_len) {
        // allocate
        cur_block_.reserve(block_size);
        cur_refs_.reserve(block_size);
        cur_literals_.reserve(block_size);
        cur_lens_.reserve(block_size);

        reset_universe();

        // encode header
        tdc::code::Binary::encode(*out_, block_size, tdc::code::Universe::of<uint64_t>());
    }

    ~PhraseBlockWriter() {
        // potentially flush final block
        flush();
    }

    void write_ref(Ref const x) {
        check_overflow();

        cur_block_.push_back(TYPE_REF);
        cur_refs_.push_back(x);

        ref_min_ = std::min(ref_min_, x);
        ref_max_ = std::max(ref_max_, x);
    }

    void write_literal(char const c) {
        check_overflow();

        cur_block_.push_back(TYPE_LIT);
        cur_literals_.push_back((Char)c);
    }

    void write_len(Ref const len) {
        check_overflow();

        cur_block_.push_back(TYPE_LEN);
        cur_lens_.push_back(len);
    }

    void flush() {
        if(!cur_block_.empty()) {
            flush_block();
        }
    }
};
