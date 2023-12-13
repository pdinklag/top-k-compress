#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <ankerl/unordered_dense.h>

#include "space_saving.hpp"

template<bool hash_len_ = false>
class TopKStringsMisraGries {
public:
    using Fingerprint = uint64_t;
    using Length = uint32_t;

    using FilterIndex = uint32_t;

private:
    struct FilterEntry;
    static constexpr auto NIL = SpaceSaving<FilterEntry>::NIL;

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
            FilterIndex next_;
            FilterIndex prev_;

        public:
            FilterEntry() : freq_(0), prev_(NIL), next_(NIL) {
            }

            FilterEntry(Hash const hash, FilterIndex const freq) : hash_(hash), freq_(freq), prev_(NIL), next_(NIL) {
            }

            // SpaceSavingItem
            FilterIndex freq() const ALWAYS_INLINE { return freq_; }
            FilterIndex prev() const ALWAYS_INLINE { return prev_; }
            FilterIndex next() const ALWAYS_INLINE { return next_; }
            
            bool is_linked() const ALWAYS_INLINE { return true; }

            void freq(FilterIndex const f) ALWAYS_INLINE { freq_ = f; }
            void prev(FilterIndex const x) ALWAYS_INLINE { prev_ = x; }
            void next(FilterIndex const x) ALWAYS_INLINE { next_ = x; }

            Hash hash() const ALWAYS_INLINE { return hash_; }
            void hash(Hash const hash) ALWAYS_INLINE { hash_ = hash; }
    } __attribute__((packed));

    FilterIndex k_;
    FilterIndex size_;

    std::unique_ptr<FilterEntry[]> filter_;
    ankerl::unordered_dense::map<Hash, FilterIndex> filter_map_;

    SpaceSaving<FilterEntry> space_saving_;
    
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

        // remove from table
        auto const h = filter_[i].hash();
        filter_map_.erase(h);
        --size_;
        assert(filter_map_.size() == size_);
    }

public:
    TopKStringsMisraGries(FilterIndex const k, size_t const sketch_rows, size_t const sketch_columns)
        : k_(k),
          size_(0),
          filter_(std::make_unique<FilterEntry[]>(k)),
          space_saving_(filter_.get(), 0, k-1, sketch_columns-1) {

        filter_map_.reserve(k);
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

                filter_[slot] = FilterEntry(h, 1);
                space_saving_.link(slot);
                filter_map_.emplace(h, slot);
                ++size_;
                assert(filter_map_.size() == size_);
                return true;
            } else {
                // filter is full, try to recycle garbage
                if(space_saving_.get_garbage(slot)) {
                    // got something, recycle
                    erase(slot);
                    assert(size_ == k_ - 1);

                    filter_[slot].hash(h);
                    space_saving_.increment(slot);
                    filter_map_.emplace(h, slot);
                    ++size_;
                    assert(size_ == k_);
                    assert(filter_map_.size() == size_);
                    return true;
                } else {
                    // nothing to recycle, decrement all
                    space_saving_.decrement_all();
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
