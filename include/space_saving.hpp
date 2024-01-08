#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <functional>
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

template<SpaceSavingItem T, bool track_min_ = false>
class SpaceSaving {
private:
    using Index = typename T::Index;

    static constexpr size_t renorm_divisor_ = 2;

public:
    struct RenormalizeFunc {
        Index base;
        Index operator()(Index const f) const { return (f - base) / renorm_divisor_; }
    };
    
    static constexpr Index NIL = -1;

private:
    T* items_;
    size_t beg_;
    size_t end_;

    std::unique_ptr<Index[]> bucket_head_;
    Index threshold_;
    
    Index max_allowed_frequency_;
    Index min_frequency_;

    Index num_renormalize_;

    void preprend_list(Index const old_head, Index const new_head) {
        if(old_head != NIL) {
            assert(old_head >= beg_);
            assert(old_head <= end_);
            assert(new_head >= beg_);
            assert(new_head <= end_);

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
        RenormalizeFunc renormalize { threshold_ };

        for(Index i = beg_; i <= end_; i++) {
            auto const f = std::max(items_[i].freq(), threshold_); // nb: we must NOT allow frequency below the threshold, that would cause negative frequencies
            items_[i].freq(renormalize(f));
        }

        // compact buckets
        auto compacted_buckets = std::make_unique<Index[]>(max_allowed_frequency_ + 1);
        for(size_t f = 0; f <= max_allowed_frequency_; f++) {
            compacted_buckets[f] = NIL;
        }

        for(size_t f = 0; f <= max_allowed_frequency_; f++) {
            auto const head = bucket_head_[f];
            if(head != NIL) {
                assert(head >= beg_);
                assert(head <= end_);

                auto const adjusted_f = renormalize(f);
                preprend_list(compacted_buckets[adjusted_f], head);
                compacted_buckets[adjusted_f] = head;
            }
        }
        bucket_head_ = std::move(compacted_buckets);
    
        if constexpr(track_min_) min_frequency_ = renormalize(min_frequency_);

        // reset threshold
        threshold_ = 0;

        // callback
        if(on_renormalize) on_renormalize(renormalize);

        // keep count of renormalizations
        ++num_renormalize_;
    }

public:
    std::function<void(RenormalizeFunc)> on_renormalize;

    inline SpaceSaving(T* items, Index const begin, Index const end, Index const max_allowed_frequency)
        : items_(items), beg_(begin), end_(end), threshold_(0), min_frequency_(NIL), max_allowed_frequency_(max_allowed_frequency), num_renormalize_(0) {

        assert(beg_ <= end_);
        assert(max_allowed_frequency_ > 1);

        // initialize buckets
        bucket_head_ = std::make_unique<Index[]>(max_allowed_frequency_ + 1);
        for(Index f = 0; f <= max_allowed_frequency_; f++) {
            bucket_head_[f] = NIL;
        }
    }

    void init_garbage() {
        // link items in garbage bucket
        bucket_head_[threshold_] = beg_;

        for(Index i = beg_; i <= end_; i++) {
            items_[i].prev((i > beg_)     ? (i - 1) : NIL);
            items_[i].next((i < end_ - 1) ? (i + 1) : NIL);
        }
    }

    bool get_garbage(Index& out_v) const ALWAYS_INLINE {
        out_v = bucket_head_[threshold_];
        return out_v != NIL;
    }

    void increment(Index const v) ALWAYS_INLINE {
        assert(v >= beg_);
        assert(v <= end_);

        // get frequency, assuring that it is >= threshold
        auto const f = std::max(items_[v].freq(), threshold_);
        assert(f <= max_allowed_frequency_);

        if(items_[v].is_linked()) {
            // unlink from wherever it is currently linked at
            unlink(v);

            // re-link as head of next bucket
            auto const u = bucket_head_[f+1];
            if(u != NIL) {
                assert(u >= beg_);
                assert(u <= end_);
                assert(items_[u].prev() == NIL);

                items_[v].next(u);
                items_[u].prev(v);
            }
            bucket_head_[f+1] = v;
        }

        // increment frequency
        items_[v].freq(f + 1);

        // potentially set minimum frequency
        if constexpr(track_min_) {
            if(min_frequency_ == NIL) min_frequency_ = f + 1;
        }

        // possibly renormalize
        if(f + 1 == max_allowed_frequency_) {
            renormalize();
        }
    }

    void decrement_all() ALWAYS_INLINE {
        // if current threshold bucket exists, prepend all its nodes to the next bucket
        auto const head = bucket_head_[threshold_];
        if(head != NIL) {
            assert(head >= beg_);
            assert(head <= end_);

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
        assert(v >= beg_);
        assert(v <= end_);

        auto const f = std::max(items_[v].freq(), threshold_); // make sure frequency is at least threshold
        if(f >= max_allowed_frequency_) {
            // we are trying to directly insert something with a too large frequency
            // renormalize until the frequency matches and then call link again
            auto const& item = items_[v];
            while(item.freq() >= max_allowed_frequency_) {
                renormalize();
            }
            return;
        }
        assert(f <= max_allowed_frequency_);

        // make new head of bucket
        auto const u = bucket_head_[f];
        if(u != NIL) {
            assert(u >= beg_);
            assert(u <= end_);
            assert(items_[u].prev() == NIL);

            items_[v].next(u);
            items_[u].prev(v);
        }
        bucket_head_[f] = v;

        if constexpr(track_min_) {
            // f may be a new minimum
            if(min_frequency_ == NIL || f < min_frequency_) {
                min_frequency_ = f;
            }
            assert(min_frequency_ != NIL);
        }
    }

    void unlink(Index const v) ALWAYS_INLINE {
        assert(v >= beg_);
        assert(v <= end_);

        // remove
        auto const x = items_[v].prev();
        auto const y = items_[v].next();

        items_[v].prev(NIL);
        items_[v].next(NIL);

        if(x != NIL) items_[x].next(y);
        if(y != NIL) items_[y].prev(x);

        // if v was the head of its bucket, update the bucket
        auto const f = items_[v].freq();
        assert(f <= max_allowed_frequency_);
        if(bucket_head_[f] == v) {
            assert(x == NIL);

            // move bucket head to next (maybe NIL)
            bucket_head_[f] = y;

            if constexpr(track_min_) {
                if(f == min_frequency_ && y == NIL) {
                    // the last item from the minimum bucket was removed
                    // we need to find a new minimum
                    min_frequency_ = NIL;

                    // FIXME: in the worst case, v was the only item in the data structure at all
                    // we then scan all possible frequencies despite guaranteed not to find anything
                    // this could be resolved by tracking the number of linked items, or alternatively the current maximum frequency
                    for(auto mf = f + 1; mf <= max_allowed_frequency_; mf++) {
                        if(bucket_head_[mf] != NIL) {
                            min_frequency_ = mf;
                            break;
                        }
                    }
                }
            }
        }
    }

    Index threshold() const ALWAYS_INLINE {
        return threshold_;
    }

    // nb: the min frequency is NOT "threshold-corrected" in any way, and it is undefined if nothing was ever linked
    Index min_frequency() ALWAYS_INLINE {
        if constexpr(track_min_) {
            assert(min_frequency_ != NIL);
            return min_frequency_;
        } else {
            // not supported
            assert(false);
            return NIL;
        }
    }

    Index extract_min() ALWAYS_INLINE {
        if constexpr(track_min_) {
            assert(min_frequency_ != NIL);
            auto const v = bucket_head_[min_frequency_];
            assert(v != NIL);
            unlink(v);
            return v;
        } else {
            // not supported
            assert(false);
            return NIL;
        }
    }

    void print_debug_info() const {
        std::cout << "# DEBUG: space-saving << threshold=" << threshold_ << ", num_renormalize=" << num_renormalize_ << std::endl;
    }
};
