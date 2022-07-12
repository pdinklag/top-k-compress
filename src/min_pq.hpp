#include <algorithm>
#include <cassert>
#include <concepts>
#include <list>

template<std::unsigned_integral Frequency>
class MinPQ {
private:
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
            size_t const item = *former.entry;

            // find next bucket
            auto const cur_freq = former.bucket->freq;
            auto next = former.bucket;
            ++next;

            if(next == buckets_.end() || next->freq > cur_freq + 1) {
                // frequency of next bucket too large or current bucket was last bucket, insert new bucket with proper frequency
                next = buckets_.emplace(next, cur_freq + 1);
            }
            assert(next->freq == cur_freq + 1);

            // remove item, and possibly its bucket if it is empty
            remove(former);

            // insert item into next bucket
            next->items.emplace_front(item);

            // return new location
            return { next, next->items.begin() };
        } else {
            return former;
        }
    }

    Location remove(Location const& what) {
        if(what) {
            // remove item from bucket
            what.bucket->items.erase(what.entry);

            // delete bucket if empty
            if(what.bucket->empty()) {
                buckets_.erase(what.bucket);
            }
        }

        // return invalid location
        return {};
    }

    Location insert(size_t const item, Frequency const freq) {
        // find first bucket with frequency greater or equal to given frequency
        auto bucket = std::find_if(buckets_.begin(), buckets_.end(), [&](Bucket const& bucket){ return bucket.freq >= freq; });

        // maybe insert bucket
        if(bucket == buckets_.end() || bucket->freq > freq) {
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
        assert(!buckets_.empty());
        
        auto& min_bucket = buckets_.front();
        assert(!min_bucket.empty());

        size_t const item = min_bucket.items.front();
        min_bucket.items.pop_front();

        // delete bucket if empty
        if(min_bucket.empty()) {
            buckets_.pop_front();
        }

        return item;
    }

    Frequency freq(Location const& what) {
        return what.bucket->freq;
    }
};
