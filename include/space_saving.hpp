#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <iostream>
#include <memory>

#include "always_inline.hpp"

template<typename T>
concept SpaceSavingItem =
    requires { typename T::Index; } &&
    requires(T const& item) {
        { item.freq() } -> std::convertible_to<typename T::Index>;
        { item.prev() } -> std::convertible_to<typename T::Index>;
        { item.next() } -> std::convertible_to<typename T::Index>;
        { item.is_linked() } -> std::same_as<bool>;
    } &&
    requires(T& item, typename T::Index const x) {
        { item.freq(x) };
        { item.prev(x) };
        { item.next(x) };
    };

template<SpaceSavingItem T>
class SpaceSaving {
private:
    using Index = typename T::Index;

    static constexpr size_t renorm_divisor_ = 2;
    static constexpr Index NIL = -1;

    T* items_;
    size_t num_;

    std::unique_ptr<Index[]> bucket_head_;
    Index threshold_;
    
    Index max_allowed_frequency_;

    void preprend_list(Index const old_head, Index const new_head) {
        if(old_head != NIL) {
            // find tail of preprended list
            auto link = new_head;
            while(items_[link].next() != NIL) {
                link = items_[link].next();
            }

            // re-chain contents
            items_[link].next(old_head);
            items_[old_head].prev(link);
        }
    }

    void renormalize() {
        // we normalize the frequency to [0, renorm_divisor_ * max_allowed_frequency_]
        auto renormalize = [&](size_t const f){ return (f - threshold_) / renorm_divisor_; };

        for(size_t i = 1; i < num_; i++) {
            auto const f = std::max(items_[i].freq(), threshold_); // nb: we must NOT allow frequency below the threshold, that would cause negative frequencies
            items_[i].freq(renormalize(f));
        }

        // compact buckets
        auto compacted_buckets = std::make_unique<Index[]>(max_allowed_frequency_ + 1);
        for(size_t f = 0; f <= max_allowed_frequency_; f++) {
            auto const head = bucket_head_[f];
            if(head != NIL) {
                auto const adjusted_f = renormalize(f);
                preprend_list(compacted_buckets[adjusted_f], head);
                compacted_buckets[adjusted_f] = head;
            }
        }
        bucket_head_ = std::move(compacted_buckets);

        // reset threshold
        threshold_ = 0;
    }

public:
    inline SpaceSaving(T* items, size_t const num, Index const max_frequency, size_t const first = 1)
        : items_(items), num_(num), threshold_(0), max_allowed_frequency_(max_frequency) {

        if(max_frequency <= 1) {
            std::cerr << "max frequency must be at least two" << std::endl;
            std::abort();
        }

        // initialize buckets
        bucket_head_ = std::make_unique<Index[]>(max_allowed_frequency_ + 1);
        for(Index f = 0; f <= max_allowed_frequency_; f++) {
            bucket_head_[f] = NIL;
        }
    }

    void init_as_garbage(size_t const first, size_t const last) {
        assert(first <= last);
        assert(last < num_);

        // link items in garbage bucket
        bucket_head_[0] = first;

        for(Index i = first; i <= last; i++) {
            items_[i].prev((i > 1)        ? (i - 1) : NIL);
            items_[i].next((i < num_ - 1) ? (i + 1) : NIL);
        }
    }

    bool get_garbage(Index& out_v) const ALWAYS_INLINE {
        out_v = bucket_head_[threshold_];
        return out_v != NIL;
    }

    void increment(Index const v) ALWAYS_INLINE {
        // get frequency, assuring that it is >= threshold
        auto const f = std::max(items_[v].freq(), threshold_);

        if(items_[v].is_linked()) {
            // unlink from wherever it is currently linked at
            unlink(v);

            // re-link as head of next bucket
            auto const u = bucket_head_[f+1];
            if(u != NIL) {
                assert(items_[u].prev() == NIL);

                items_[v].next(u);
                items_[u].prev(v);
            }
            bucket_head_[f+1] = v;
        }

        // increment frequency
        items_[v].freq(f + 1);

        // possibly renormalize
        if(f + 1 == max_allowed_frequency_) {
            renormalize();
        }
    }

    void decrement_all() ALWAYS_INLINE {
        // if current threshold bucket exists, prepend all its nodes to the next bucket
        auto const head = bucket_head_[threshold_];
        if(head != NIL) {
            // prepend all to next bucket
            preprend_list(bucket_head_[threshold_ + 1], head);
            bucket_head_[threshold_ + 1] = head;

            // delete threshold bucket
            bucket_head_[threshold_] = NIL;
        }

        // then simply increment the threshold
        ++threshold_;
    }

    void link(Index const v) ALWAYS_INLINE {
        auto const f = std::max(items_[v].freq(), threshold_); // make sure frequency is at least threshold

        // make new head of bucket
        auto const u = bucket_head_[f];
        if(u != NIL) {
            assert(items_[u].prev() == NIL);

            items_[v].next(u);
            items_[u].prev(v);
        }
        bucket_head_[f] = v;
    }

    void unlink(Index const v) ALWAYS_INLINE {
        // remove
        auto const x = items_[v].prev();
        auto const y = items_[v].next();

        items_[v].prev(NIL);
        items_[v].next(NIL);

        if(x != NIL) items_[x].next(y);
        if(y != NIL) items_[y].prev(x);

        // if v was the head of its bucket, update the bucket
        auto const f = items_[v].freq();
        if(bucket_head_[f] == v) {
            assert(x == NIL);

            // move bucket head to next (maybe NIL)
            bucket_head_[f] = y;
        }
    }

    Index threshold() const ALWAYS_INLINE {
        return threshold_;
    }
};
