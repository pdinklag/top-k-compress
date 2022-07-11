#include <algorithm>
#include <concepts>
#include <list>

template<std::unsigned_integral Frequency>
class MinPQ {
private:
    struct Bucket {
        Frequency freq;

        std::list<size_t> non_maximal;
        std::list<size_t> maximal;

        Bucket(Frequency f) : freq(f) {
        }

        std::list<size_t>& list(bool maximal) {
            return maximal ? this->maximal : non_maximal;
        }

        bool empty() const {
            return non_maximal.empty() && maximal.empty();
        }
    };

    std::list<Bucket> buckets_;

public:
    struct Location {
        std::list<Bucket>::iterator bucket;
        std::list<size_t>::iterator entry;
        bool maximal;
    };

    MinPQ() {
    }

    Location increase_key(Location const& former) {
        // 
        size_t const item = *former.entry;

        // find next bucket
        auto const cur_freq = former.bucket->freq;
        auto next = former.bucket;
        ++next;

        if(next == buckets_.end() || next->freq > cur_freq + 1) {
            // frequency of next bucket too large or current bucket was last bucket, insert new bucket with proper frequency
            next = buckets_.insert(next, Bucket(cur_freq + 1));
        }

        // remove item from former bucket
        former.bucket->list(former.maximal).erase(former.entry);

        // maybe delete former bucket
        if(former.bucket->empty()) {
            buckets_.erase(former.bucket);
        }

        // insert item into next bucket
        auto& new_list = next->list(former.maximal);
        new_list.push_front(item);

        // return new location
        return { next, new_list.begin(), former.maximal };
    }

    Location insert(size_t const item, Frequency const freq, bool const maximal) {
        // find first bucket with frequency greater or equal to given frequency
        auto bucket = std::find_if(buckets_.begin(), buckets_.end(), [&](Bucket const& bucket){ return bucket.freq >= freq; });

        // maybe insert bucket
        if(bucket == buckets_.end() || bucket->freq > freq) {
            bucket = buckets_.insert(bucket, Bucket(freq));
        }

        // insert item
        auto& list = bucket->list(maximal);
        list.push_front(item);

        // return insert location
        return { bucket, list.begin(), maximal };
    }

    Location mark_non_maximal(Location const& former) {
        if(former.maximal) {
            // remove from maximal list
            size_t const item = *former.entry;
            former.bucket->maximal.erase(former.entry);

            // insert into non-maximal list
            former.bucket->non_maximal.push_front(item);

            // return new location
            return { former.bucket, former.bucket->non_maximal.begin(), false };
        } else {
            return former;
        }
    }
};
