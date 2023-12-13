#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <ankerl/unordered_dense.h>

#include "count_min2.hpp"
#include "space_saving.hpp"

template<bool hash_len_ = true>
class TopKStrings {
public:
    using Fingerprint = uint64_t;
    using Length = uint32_t;

    using FilterIndex = uint32_t;

private:
    struct FilterEntry;
    static constexpr auto NIL = SpaceSaving<FilterEntry, true>::NIL;

    using Hash = uint64_t;
    using BitPack = uint64_t;

    static constexpr Hash hash(Fingerprint const fp, Length const len) {
        if constexpr(hash_len_) {
            return Hash(len) * 68719476377ULL + Hash(fp) * 2621271ULL;
        } else {
            return Hash(fp);
        }
    }

    class FilterEntry {
        public:
            using Index = FilterIndex;

        private:
            Hash hash_;
            FilterIndex freq_;
            FilterIndex insert_freq_;
            FilterIndex next_;
            FilterIndex prev_;

        public:
            FilterEntry() : hash_(0), freq_(0), prev_(NIL), next_(NIL), insert_freq_(0) {
            }

            FilterEntry(Hash const h) : hash_(h), freq_(1), prev_(NIL), next_(NIL), insert_freq_(0) {
            }

            FilterEntry(Hash const h, FilterIndex const freq) : hash_(h), freq_(freq), prev_(NIL), next_(NIL), insert_freq_(freq) {
            }

            // SpaceSavingItem
            FilterIndex freq() const ALWAYS_INLINE { return freq_; }
            FilterIndex prev() const ALWAYS_INLINE { return prev_; }
            FilterIndex next() const ALWAYS_INLINE { return next_; }
            
            bool is_linked() const ALWAYS_INLINE { return true; }

            void freq(FilterIndex const f) ALWAYS_INLINE { freq_ = f; }
            void prev(FilterIndex const x) ALWAYS_INLINE { prev_ = x; }
            void next(FilterIndex const x) ALWAYS_INLINE { next_ = x; }

            // additional data
            FilterIndex insert_freq() const ALWAYS_INLINE { return insert_freq_; }
            void insert_freq(FilterIndex const f) ALWAYS_INLINE { insert_freq_ = f; }

            Hash hash() const ALWAYS_INLINE { return hash_; }
            void fingerprint(Hash const h) ALWAYS_INLINE { hash_ = h; }
    } __attribute__((packed));

    FilterIndex k_;
    FilterIndex size_;

    std::unique_ptr<FilterEntry[]> filter_;
    ankerl::unordered_dense::map<Hash, FilterIndex> filter_map_;

    SpaceSaving<FilterEntry, true> space_saving_;
    CountMin2<size_t> sketch_;

    bool find(Fingerprint const fp, Length const len, Hash& h, FilterIndex& out_slot) const {
        h = hash(fp, len);
        auto it = filter_map_.find(h);
        if(it != filter_map_.end()) {
            out_slot = it->second;
            return true;
        } else {
            return false;
        }
    }

    void erase(FilterIndex const i) {
        assert(size_ > 0);
        
        // sketch frequency delta
        auto const h = filter_[i].hash();
        sketch_.increment(h, filter_[i].freq() - filter_[i].insert_freq());

        // remove from table
        filter_map_.erase(h);
        --size_;
        assert(filter_map_.size() == size_);
    }

public:
    TopKStrings(FilterIndex const k, size_t const sketch_rows, size_t const sketch_columns)
        : k_(k),
          size_(0),
          filter_(std::make_unique<FilterEntry[]>(k)),
          space_saving_(filter_.get(), 0, k-1, k),
          sketch_(sketch_columns) {

        filter_map_.reserve(k);

        space_saving_.on_renormalize = [&](auto renormalize){
            // renormalize insert frequencies
            for(size_t i = 0; i < size_; i++) {
                auto& e = filter_[i];
                e.insert_freq(renormalize(e.insert_freq()));
            }

            // renormalize sketch entries
            auto const num_entries = sketch_.num_columns() * sketch_.num_rows();
            auto* table = sketch_.table();
            for(size_t i = 0; i < num_entries; i++) {
                table[i] = renormalize(table[i]);
            }
        };
    }

    FilterIndex k() const { return k_; }
    FilterIndex size() const { return size_; }

    size_t freq(size_t const slot) { return filter_[slot].freq; }

    void insert(Fingerprint const fp, Length const len) {
        FilterIndex discard;
        insert(fp, len, discard);
    }

    bool insert(Fingerprint const fp, Length const len, FilterIndex& slot) {
        Hash h;
        if(find(fp, len, h, slot)) {
            // string is frequent, increment
            space_saving_.increment(slot);
            return true;
        } else {
            // string is not frequent
            if(size_ < k_) {
                // filter is not yet full, insert
                slot = size_;

                filter_[slot] = FilterEntry(h);
                space_saving_.link(slot);
                filter_map_.emplace(h, slot);
                ++size_;
                assert(filter_map_.size() == size_);
                return true;
            } else {
                // filter is full, sketch
                auto const est = sketch_.increment_and_estimate(h, 1);
                auto const swap = (est > space_saving_.min_frequency());
                if(swap) {
                    // estimated frequency is higher than minimum, swap!

                    // erase minimum
                    slot = space_saving_.extract_min();
                    erase(slot);
                    assert(size_ == k_ - 1);

                    // move from sketch
                    filter_[slot] = FilterEntry(h, est);
                    space_saving_.link(slot);
                    filter_map_.emplace(h, slot);
                    ++size_;
                    assert(size_ == k_);
                    assert(filter_map_.size() == size_);
                    return true;
                }
            }
        }
        return false;
    }

    // attempts to find the given string
    // if it is found, out_slot will contain the slot number
    // otherwise, out_slot will contain the slot number at which the string would be inserted
    bool find(Fingerprint const fp, Length const len, FilterIndex& out_slot) const {
        Hash h;
        return find(fp, len, h, out_slot);
    }
};
