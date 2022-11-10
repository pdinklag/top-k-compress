#pragma once

#include <algorithm>
#include <limits>
#include <vector>

#include <iostream>
#include <unordered_set>

#include <iopp/concepts.hpp>

#include <tdc/code/universal/binary.hpp>
#include <tdc/code/universal/elias_delta.hpp>

template<iopp::BitSink Out, std::unsigned_integral Ref = uint32_t>
class PhraseBlockWriter {
private:
    static constexpr bool DEBUG = false;

    using ItemType = uint8_t;

    static constexpr ItemType TYPE_REF = 0;
    static constexpr ItemType TYPE_LIT = 1;
    static constexpr ItemType TYPE_LEN = 2;

    Out* out_;
    
    size_t block_size_;
    bool use_len_;
    std::vector<Ref> cur_refs_;
    std::vector<Ref> cur_lens_;
    std::vector<char> cur_literals_;
    std::vector<ItemType> cur_block_;

    Ref ref_min_, ref_max_;
    Ref len_min_, len_max_;
    uint8_t lit_min_, lit_max_;

    void flush_block() {
        // encode block header
        tdc::code::Binary::encode(*out_, ref_min_, tdc::code::Universe::of<Ref>());
        tdc::code::Binary::encode(*out_, ref_max_, tdc::code::Universe::of<Ref>());

        if(use_len_) {
            tdc::code::EliasDelta::encode(*out_, len_min_, tdc::code::Universe::of<Ref>());
            tdc::code::EliasDelta::encode(*out_, len_max_ - len_min_, tdc::code::Universe::of<Ref>());            
        }

        tdc::code::EliasDelta::encode(*out_, lit_min_, tdc::code::Universe::of<uint8_t>());
        tdc::code::EliasDelta::encode(*out_, lit_max_ - lit_min_, tdc::code::Universe::of<uint8_t>());

        tdc::code::Universe u_refs(ref_min_, ref_max_);
        tdc::code::Universe u_lens(len_min_, len_max_);
        tdc::code::Universe u_lits(lit_min_, lit_max_);

        if constexpr(DEBUG) {
            std::unordered_set<char> unique_literals;
            for(auto const c : cur_literals_) unique_literals.emplace(c);

            std::unordered_set<uint64_t> unique_refs;
            for(auto const x : cur_refs_) unique_refs.emplace(x);

            std::cout << "block: size=" << cur_block_.size()
                << ", num_literals=" << cur_literals_.size()
                << ", unique_literals=" << unique_literals.size()
                << ", literal_entropy=" << u_lits.entropy()
                << ", num_refs=" << cur_refs_.size()
                << ", unique_refs=" << unique_refs.size()
                << ", ref_entropy=" << u_refs.entropy()
                << std::endl;
        }

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
                    tdc::code::Binary::encode(*out_, cur_literals_[next_lit++], u_lits);
                    break;

                case TYPE_LEN:
                    tdc::code::Binary::encode(*out_, cur_lens_[next_len++], u_lens);
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

        lit_min_ = UINT8_MAX;
        lit_max_ = 0;

        len_min_ = std::numeric_limits<Ref>::max();
        len_max_ = 0;
    }

public:
    PhraseBlockWriter(Out& out, size_t const block_size, bool const use_len = false) : out_(&out), block_size_(block_size), use_len_(use_len) {
        // allocate
        cur_block_.reserve(block_size);
        cur_refs_.reserve(block_size);
        cur_literals_.reserve(block_size);

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
        cur_literals_.push_back(c);

        uint8_t const x = c;
        lit_min_ = std::min(lit_min_, x);
        lit_max_ = std::max(lit_max_, x);
    }

    void write_len(Ref const len) {
        check_overflow();

        cur_block_.push_back(TYPE_LEN);
        cur_lens_.push_back(len);

        len_min_ = std::min(len_min_, len);
        len_max_ = std::max(len_max_, len);
    }

    void flush() {
        if(!cur_block_.empty()) {
            flush_block();
        }
    }
};
