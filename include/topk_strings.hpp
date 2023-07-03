#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <word_packing.hpp>

#include <ankerl/unordered_dense.h>

#include "count_min.hpp"
#include "min_pq.hpp"

class TopKStrings {
public:
    using Fingerprint = uint64_t;
    using Length = uint32_t;

    using Index = uint32_t;

private:
    using Hash = uint64_t;
    using BitPack = uint64_t;

    static constexpr Hash hash(Fingerprint const fp, Length const len) {
        return Hash(len) * 68719476377ULL + Hash(fp) * 2621271ULL;
    }

    Index k_;
    Index size_;

    MinPQ<size_t> min_pq_;
    CountMin<size_t> sketch_;

    struct FilterEntry {
        Hash h;
        size_t freq;
        size_t insert_freq;
        MinPQ<size_t>::Location min_pq_loc;
    } __attribute__((packed));

    ankerl::unordered_dense::map<Hash, Index> filter_map_;
    std::unique_ptr<FilterEntry[]> filter_;
    std::unique_ptr<BitPack[]> used_;

    bool find(Fingerprint const fp, Length const len, Hash& h, Index& out_slot) const {
        auto used = word_packing::bit_accessor(used_.get());

        h = hash(fp, len);
        auto it = filter_map_.find(h);
        if(it != filter_map_.end()) {
            out_slot = it->second;
            assert(used[out_slot]);
            return true;
        } else {
            return false;
        }
    }

    void erase(Index i) {
        assert(size_ > 0);
        auto used = word_packing::bit_accessor(used_.get());
        assert(used[i]);
        
        // sketch frequency delta
        auto const h = filter_[i].h;
        sketch_.increment(h, filter_[i].freq - filter_[i].insert_freq);

        // remove from table
        filter_map_.erase(h);
        --size_;
        assert(filter_map_.size() == size_);

        used[i] = false;
    }

public:
    TopKStrings(Index const k, size_t const sketch_rows, size_t const sketch_columns)
        : k_(k),
          size_(0),
          min_pq_(k),
          sketch_(sketch_rows, sketch_columns) {

        filter_ = std::make_unique<FilterEntry[]>(k);
        filter_map_.reserve(k);

        auto const num_packs = word_packing::num_packs_required<BitPack>(k, 1);
        used_ = std::make_unique<BitPack[]>(num_packs);
        for(size_t i = 0; i < num_packs; i++) {
            used_[i] = 0;
        }
    }

    void insert(Fingerprint const fp, Length const len) {
        Hash h;
        Index i;
        if(find(fp, len, h, i)) {
            // string is frequent, increment
            ++filter_[i].freq;
            filter_[i].min_pq_loc = min_pq_.increase_key(filter_[i].min_pq_loc);
        } else {
            // string is not frequent
            auto used = word_packing::bit_accessor(used_.get());

            if(size_ < k_) {
                // filter is not yet full, insert
                i = size_;

                FilterEntry e;
                e.h = h;
                e.freq = 1;
                e.insert_freq = 0;
                e.min_pq_loc = min_pq_.insert(i, 1);

                used[i] = 1;
                filter_[i] = e;
                filter_map_.emplace(h, i);
                ++size_;
                assert(filter_map_.size() == size_);
            } else {
                // filter is full, sketch
                auto const est = sketch_.increment_and_estimate(h, 1);
                if(est > min_pq_.min_frequency()) {
                    // estimated frequency is higher than minimum, swap!

                    // erase minimum
                    i = min_pq_.extract_min();
                    erase(i);
                    assert(size_ == k_ - 1);
                    assert(!used[i]);

                    // move from sketch
                    FilterEntry e;
                    e.h = h;
                    e.freq = est;
                    e.insert_freq = est;
                    e.min_pq_loc = min_pq_.insert(i, est);

                    used[i] = 1;
                    filter_[i] = e;
                    filter_map_.emplace(h, i);
                    ++size_;
                    assert(size_ == k_);
                    assert(filter_map_.size() == size_);
                }
            }
        }
    }

    // attempts to find the given string
    // if it is found, out_slot will contain the slot number
    // otherwise, out_slot will contain the slot number at which the string would be inserted
    bool find(Fingerprint const fp, Length const len, Index& out_slot) const {
        Hash h;
        return find(fp, len, h, out_slot);
    }
};
