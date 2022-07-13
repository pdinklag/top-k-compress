#include <algorithm>
#include <cassert>
#include <concepts>
#include <list>

template<std::unsigned_integral Frequency>
class MinPQ {
private:
    static constexpr bool gather_stats_ = true;
    struct Stats {
        size_t num_bucket_inserts;
        size_t num_bucket_deletes;
        size_t num_increase_key;
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

    struct Bucket {
        Frequency freq;
        std::list<size_t> items;

        Bucket(Frequency f) : freq(f) {
        }

        bool empty() const {
            return items.empty();
        }
    };

    std::list<Bucket> buckets_;
    Stats stats_;

public:
    struct Location {
        std::list<Bucket>::iterator bucket;
        std::list<size_t>::iterator entry;

        inline operator bool() const {
            return bucket._M_node;
        }
    };

    MinPQ() {
    }

    Location increase_key(Location const& former) {
        // 
        if(former) {
            if constexpr(gather_stats_) ++stats_.num_increase_key;
            size_t const item = *former.entry;

            // find next bucket
            auto const cur_freq = former.bucket->freq;
            auto next = former.bucket;
            ++next;

            if(next == buckets_.end() || next->freq > cur_freq + 1) {
                // frequency of next bucket too large or current bucket was last bucket, insert new bucket with proper frequency
                if constexpr(gather_stats_) ++stats_.num_bucket_inserts;
                next = buckets_.emplace(next, cur_freq + 1);
            }
            assert(next->freq == cur_freq + 1);

            // remove item, and possibly its bucket if it is empty
            remove<false>(former);

            // insert item into next bucket
            next->items.emplace_front(item);

            // return new location
            return { next, next->items.begin() };
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
        return {};
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
            bucket = buckets_.emplace(bucket, freq);
        }

        // insert item
        assert(bucket->freq == freq);
        bucket->items.emplace_front(item);

        // return insert location
        return { bucket, bucket->items.begin() };
    }

    size_t min_frequency() const {
        assert(!buckets_.empty());
        auto& min_bucket = buckets_.front();
        return min_bucket.freq;
    }

    size_t extract_min() {
        if constexpr(gather_stats_) ++stats_.num_extract_min;
        assert(!buckets_.empty());
        
        auto& min_bucket = buckets_.front();
        assert(!min_bucket.empty());

        size_t const item = min_bucket.items.front();
        min_bucket.items.pop_front();

        // delete bucket if empty
        if(min_bucket.empty()) {
            if constexpr(gather_stats_) ++stats_.num_bucket_deletes;
            buckets_.pop_front();
        }

        return item;
    }

    Frequency freq(Location const& what) {
        return what.bucket->freq;
    }

    void print_debug_info() const {
        std::cout << "min pq info"
                  << ": num_bucket_inserts=" << stats_.num_bucket_inserts
                  << ", num_bucket_deletes=" << stats_.num_bucket_deletes
                  << ", num_inserts=" << stats_.num_inserts
                  << ", num_insert_search_steps=" << stats_.num_insert_search_steps
                  << ", num_deletes=" << stats_.num_deletes
                  << ", num_increase_key=" << stats_.num_increase_key
                  << ", num_extract_min=" << stats_.num_extract_min
                  << std::endl;
    }
};
