#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <functional>
#include <iostream>
#include <memory>

#include "always_inline.hpp"
#include "linked_list.hpp"

template<typename T>
concept SpaceSavingItem =
    LinkedListItem<T> &&
    requires(T const& item) {
        { item.freq() } -> std::convertible_to<typename T::Index>;
        { item.is_linked() } -> std::same_as<bool>;
    } &&
    requires(T& item, typename T::Index const x) {
        { item.freq(x) };
    };

template<SpaceSavingItem T, bool track_min_ = false>
class SpaceSaving {
private:
    using Index = typename T::Index;
    using List = LinkedList<T>;

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

    std::unique_ptr<List[]> buckets_;
    Index threshold_;
    
    Index max_allowed_frequency_;
    Index min_frequency_;

    Index num_renormalize_;

    void renormalize() {
        // we normalize the frequency to [0, renorm_divisor_ * max_allowed_frequency_]
        RenormalizeFunc renormalize { threshold_ };

        for(Index i = beg_; i <= end_; i++) {
            auto const f = std::max(items_[i].freq(), threshold_); // nb: we must NOT allow frequency below the threshold, that would cause negative frequencies
            items_[i].freq(renormalize(f));
        }

        // compact buckets
        auto compacted_buckets = std::make_unique<List[]>(max_allowed_frequency_ + 1);

        for(size_t f = 0; f <= max_allowed_frequency_; f++) {
            auto& bucket = buckets_[f];
            if(!bucket.empty()) {
                auto const adjusted_f = renormalize(f);
                compacted_buckets[adjusted_f].append(items_, bucket);
            }
        }
        buckets_ = std::move(compacted_buckets);
    
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

    SpaceSaving() : items_(nullptr), threshold_(0) {
    }

    SpaceSaving(T* items, Index const begin, Index const end, Index const max_allowed_frequency)
        : items_(items), beg_(begin), end_(end), threshold_(0), min_frequency_(NIL), max_allowed_frequency_(max_allowed_frequency), num_renormalize_(0) {

        assert(beg_ <= end_);
        assert(max_allowed_frequency_ > 1);

        // initialize buckets
        buckets_ = std::make_unique<List[]>(max_allowed_frequency_ + 1);
    }

    SpaceSaving(SpaceSaving&&) = default;
    SpaceSaving& operator=(SpaceSaving&&) = default;

    SpaceSaving(SpaceSaving const& other) {
        *this = other;
    }

    SpaceSaving& operator=(SpaceSaving const& other) {
        items_ = other.items_;
        beg_ = other.beg_;
        end_ = other.end_;
        threshold_ = other.threshold_;
        max_allowed_frequency_ = other.max_allowed_frequency_;
        min_frequency_ = other.min_frequency_;
        num_renormalize_ = 0;

        buckets_ = std::make_unique<List[]>(max_allowed_frequency_ + 1);
        for(Index f = 0; f <= max_allowed_frequency_; f++) {
            buckets_[f] = other.buckets_[f];
        }

        return *this;
    }

    void set_items(T* items) {
        items_ = items;
    }

    void init_garbage() {
        // link items in garbage bucket
        auto& garbage_bucket = buckets_[threshold_];
        for(Index i = beg_; i <= end_; i++) {
            garbage_bucket.push_front(items_, i);
        }
    }

    bool get_garbage(Index& out_v) const ALWAYS_INLINE {
        auto& garbage_bucket = buckets_[threshold_];
        if(garbage_bucket.empty()) {
            return false;
        } else {
            out_v = garbage_bucket.front();
            return true;
        }
    }

    void increment(Index const v) ALWAYS_INLINE {
        assert(v >= beg_);
        assert(v <= end_);

        // get frequency, assuring that it is >= threshold
        auto const f = std::max(items_[v].freq(), threshold_);
        assert(f <= max_allowed_frequency_);

        if(f == max_allowed_frequency_) {
            // this item already has the maximum frequency, don't increment
            return;
        }

        if(items_[v].is_linked()) {
            // unlink from wherever it is currently linked at
            unlink(v);

            // re-link in next bucket
            auto& next_bucket = buckets_[f+1];
            next_bucket.push_front(items_, v);
        }

        // increment frequency
        items_[v].freq(f + 1);

        // potentially set minimum frequency
        if constexpr(track_min_) {
            if(min_frequency_ == NIL) min_frequency_ = f + 1;
        }

        // possibly renormalize
        /*
        if(f + 1 == max_allowed_frequency_) {
            renormalize();
        }
        */
    }

    void decrement_all() ALWAYS_INLINE {
        // if current threshold bucket exists, prepend all its nodes to the next bucket
        auto& min_bucket = buckets_[threshold_];
        if(!min_bucket.empty()) {
            // prepend all to next bucket
            auto& bucket = buckets_[threshold_ + 1];
            bucket.append(items_, min_bucket);

            // delete threshold bucket
            min_bucket.clear();
        }

        // then simply increment the threshold
        ++threshold_;

        // possibly renormalize
        if(threshold_ >= max_allowed_frequency_ / 2) {
            renormalize();
        }
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
        auto& bucket = buckets_[f];
        bucket.push_front(items_, v);

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
        auto const f = items_[v].freq();
        assert(f <= max_allowed_frequency_);

        auto& bucket = buckets_[f];
        bucket.erase(items_, v);

        if constexpr(track_min_) {
            if(bucket.empty() && f == min_frequency_) {
                // the last item from the minimum bucket was removed
                // we need to find a new minimum
                min_frequency_ = NIL;

                // FIXME: in the worst case, v was the only item in the data structure at all
                // we then scan all possible frequencies despite guaranteed not to find anything
                // this could be resolved by tracking the number of linked items, or alternatively the current maximum frequency
                for(auto mf = f + 1; mf <= max_allowed_frequency_; mf++) {
                    if(!buckets_[mf].empty()) {
                        min_frequency_ = mf;
                        break;
                    }
                }
            }
        }
    }

    Index threshold() const ALWAYS_INLINE {
        return threshold_;
    }

    Index bucket_size(Index const f) const {
        return buckets_[f].size(items_);
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
            auto& bucket = buckets_[min_frequency_];
            assert(!bucket.empty());
            auto const v = bucket.front();
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
