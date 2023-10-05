#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstdint>
#include <list>

#include "list_pool.hpp"

template<std::unsigned_integral Frequency, std::unsigned_integral EntryIndex = uint32_t>
class MinPQ {
private:
    static constexpr bool gather_stats_ = false;
    struct Stats {
        size_t num_bucket_inserts;
        size_t num_bucket_deletes;
        size_t num_increase_key;
        size_t num_slides;
        size_t num_inserts;
        size_t num_insert_search_steps;
        size_t num_deletes;
        size_t num_extract_min;

        inline Stats() : num_bucket_inserts(0),
                         num_bucket_deletes(0),
                         num_increase_key(0),
                         num_inserts(0),
                         num_insert_search_steps(0),
                         num_deletes(0),
                         num_extract_min(0) {
        }
    };

    using ItemPool = ListPool<EntryIndex, EntryIndex>;

    struct Bucket {
        Frequency freq;
        ItemPool::List items;

        Bucket() : freq(0) {
        }

        Bucket(Frequency _freq, ItemPool::List&& _items) : freq(_freq), items(std::move(_items)) {
        }

        bool empty() const {
            return items.empty();
        }

        size_t size() const {
            return items.size();
        }
    } __attribute__((packed));

    using BucketPool = ListPool<Bucket, EntryIndex>;
    
    BucketPool bucket_pool_;
    ItemPool item_pool_;
    
    BucketPool::List buckets_;
    Stats stats_;

public:
    struct Location {
        using BucketRef = BucketPool::List::iterator;
        using ItemRef = ItemPool::List::iterator;

        BucketRef bucket;
        ItemRef entry;
        bool valid;

        inline Location(BucketRef _bucket, ItemRef _entry) : bucket(_bucket), entry(_entry), valid(true) {
        }

        inline Location() : valid(false) {
        }

        inline operator bool() const {
            return valid;
        }
    } __attribute__((packed));

    MinPQ(EntryIndex const max_items) : item_pool_(max_items), bucket_pool_(max_items) {
        buckets_ = bucket_pool_.new_list();
    }

    Location increase_key(Location const& former) {
        // 
        if(former) {
            if constexpr(gather_stats_) ++stats_.num_increase_key;
            EntryIndex const item = *former.entry;

            // find next bucket
            auto const cur_freq = former.bucket->freq;
            auto next = former.bucket;
            ++next;

            if(next == buckets_.end() || next->freq > cur_freq + 1) {
                // frequency of next bucket too large or current bucket was last bucket
                if(former.bucket->size() == 1) {
                    // the item is the only item in its bucket, simply increase the bucket's frequency and we're done
                    if constexpr(gather_stats_) ++stats_.num_slides;
                    
                    ++former.bucket->freq;
                    return former;
                } else {                
                    // insert new bucket with proper frequency
                    if constexpr(gather_stats_) ++stats_.num_bucket_inserts;
                    next = buckets_.emplace(next, cur_freq + 1, item_pool_.new_list());
                }
            }
            assert(next->freq == cur_freq + 1);

            // remove item, and possibly its bucket if it is empty
            remove<false>(former);

            // insert item into next bucket
            next->items.emplace_front(item);

            // return new location
            return Location(next, next->items.begin());
        } else {
            return former;
        }
    }

    template<bool count_as_delete = true>
    Location remove(Location const& what) {
        if(what) {
            if constexpr(gather_stats_ && count_as_delete) ++stats_.num_deletes;

            // remove item from bucket
            what.bucket->items.erase(what.entry);

            // delete bucket if empty
            if(what.bucket->empty()) {
                if constexpr(gather_stats_) ++stats_.num_bucket_deletes;
                buckets_.erase(what.bucket);
            }
        }

        // return invalid location
        return Location();
    }

    Location insert(size_t const item, Frequency const freq) {
        if constexpr(gather_stats_) ++stats_.num_inserts;

        // find first bucket with frequency greater or equal to given frequency
        auto bucket = buckets_.begin();
        for(; bucket != buckets_.end() && bucket->freq < freq; bucket++)  {
            if constexpr(gather_stats_) ++stats_.num_insert_search_steps;
        }

        // maybe insert bucket
        if(bucket == buckets_.end() || bucket->freq > freq) {
            if constexpr(gather_stats_) ++stats_.num_bucket_inserts;
            bucket = buckets_.emplace(bucket, freq, item_pool_.new_list());
        }

        // insert item
        assert(bucket->freq == freq);
        bucket->items.emplace_front(item);

        // return insert location
        return Location(bucket, bucket->items.begin());
    }

    size_t min_frequency() const {
        assert(!buckets_.empty());
        auto& min_bucket = buckets_.front();
        return min_bucket.freq;
    }

    EntryIndex extract_min() {
        if constexpr(gather_stats_) ++stats_.num_extract_min;
        assert(!buckets_.empty());
        
        auto& min_bucket = buckets_.front();
        assert(!min_bucket.empty());

        EntryIndex const item = min_bucket.items.front();
        min_bucket.items.pop_front();

        // delete bucket if empty
        if(min_bucket.empty()) {
            if constexpr(gather_stats_) ++stats_.num_bucket_deletes;
            buckets_.pop_front();
        }

        return item;
    }

    template<std::predicate<EntryIndex> Predicate>
    bool extract_min(Predicate pick, EntryIndex& out_item) {
        assert(!buckets_.empty());

        auto& min_bucket = buckets_.front();
        assert(!min_bucket.empty());

        for(auto it = min_bucket.items.begin(); it != min_bucket.items.end(); ++it) {
            if(pick(*it)) {
                // extract
                out_item = *it;
                min_bucket.items.erase(it);

                // delete bucket if empty
                if(min_bucket.empty()) {
                    if constexpr(gather_stats_) ++stats_.num_bucket_deletes;
                    buckets_.pop_front();
                }
                return true;
            }
        }
        return false;
    }

    Frequency freq(Location const& what) {
        return what.bucket->freq;
    }

    void print_debug_info() const {
        if constexpr(!gather_stats_) return;
        std::cout << "min pq info"
                  << ": num_bucket_inserts=" << stats_.num_bucket_inserts
                  << ", num_bucket_deletes=" << stats_.num_bucket_deletes
                  << ", num_inserts=" << stats_.num_inserts
                  << ", num_insert_search_steps=" << stats_.num_insert_search_steps
                  << ", num_deletes=" << stats_.num_deletes
                  << ", num_increase_key=" << stats_.num_increase_key
                  << ", num_slides=" << stats_.num_slides
                  << ", num_extract_min=" << stats_.num_extract_min
                  << std::endl;
    }
};
