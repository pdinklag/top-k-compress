#pragma once

#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include <ankerl/unordered_dense.h>
#include <ankerl_memory_size.hpp>

#include <idiv_ceil.hpp>
#include <word_packing.hpp>

#include "result.hpp"

template<std::unsigned_integral Key, std::unsigned_integral Value, Key sampling_>
class DynamicUniverseSampling {
private:
    static_assert(sampling_ <= UINT32_MAX);

    using TruncatedKey = std::conditional_t<sampling_ <= UINT8_MAX, uint8_t, std::conditional_t<sampling_ <= UINT16_MAX, uint16_t, uint32_t>>;

    static constexpr Key bucket_for(Key const key) { return key / sampling_; }
    static constexpr TruncatedKey truncate(Key const key) { return TruncatedKey(key % sampling_); }

    static constexpr size_t packs_per_bucket_ = word_packing::num_packs_required<uint64_t>(sampling_, 1);
    static constexpr size_t pack_bits_ = 64;

    static constexpr uint8_t lowest_set_bit(uint64_t const x) {
        return __builtin_ctzll(x);
    }

    static constexpr uint8_t highest_set_bit(uint64_t const x) {
        return pack_bits_ - 1 - __builtin_clzll(x);
    }

    struct Bucket {
        std::unique_ptr<uint64_t[]> data_;
        ankerl::unordered_dense::map<TruncatedKey, Value> values_;

        Bucket() {
            data_ = std::make_unique<uint64_t[]>(packs_per_bucket_);
            for(size_t i = 0; i < packs_per_bucket_; i++) data_[i] = 0;
        }

        auto bits() { return word_packing::bit_accessor(data_.get()); }
        auto bits() const { return word_packing::bit_accessor(data_.get()); }

        PosResult predecessor(size_t x) const {
            uint64_t const* data = data_.get();
            auto i = x / pack_bits_;

            if(data[i]) {
                // the predecessor may be in x's own pack word
                auto j = x % pack_bits_;
                auto m = 1ULL << j;

                auto const pack = data[i];
                while(m) {
                    if(pack & m) return { true, i * pack_bits_ + j };

                    m >>= 1;
                    --j;
                }
            }

            // we did not find a predecesor in the corresponding word
            // thus, the predcesor is the highest set previous bit
            while(i > 0) {
                if(data[i-1] != 0) {
                    // find the last set bit
                    return { true, (i-1) * pack_bits_ + highest_set_bit(data[i-1]) };
                }
                --i;
            }
            return PosResult::none();
        }

        PosResult successor(size_t x) const {
            uint64_t const* data = data_.get();
            auto i = x / pack_bits_;

            if(data[i]) {
                // the successor may be in x's own pack word
                auto j = x % pack_bits_;
                auto m = 1ULL << j;

                auto const pack = data[i];
                while(j < pack_bits_) {
                    if(pack & m) return { true, i * pack_bits_ + j };

                    m <<= 1;
                    ++j;
                }
            }

            // we did not find a successor in the corresponding word
            // thus, the successor is the lowest set next bit
            ++i;
            while(i < packs_per_bucket_) {
                if(data[i] != 0) {
                    // find the last set bit
                    return { true, i * pack_bits_ + lowest_set_bit(data[i]) };
                }
                ++i;
            }
            return PosResult::none();
        }

        TruncatedKey min() const {
            uint64_t const* data = data_.get();

            // find first pack that has a set bit
            for(size_t i = 0; i < packs_per_bucket_; i++) {
                if(data[i] != 0) {
                    // find the first set bit
                    return i * pack_bits_ + lowest_set_bit(data[i]);
                }
            }

            assert(false); // there must not be any empty buckets
            return TruncatedKey();
        }

        TruncatedKey max() const {
            uint64_t const* data = data_.get();

            // find last pack that has a set bit
            for(size_t i = packs_per_bucket_; i > 0; i--) {
                if(data[i-1] != 0) {
                    // find the last set bit
                    return (i-1) * pack_bits_ + highest_set_bit(data[i-1]);
                }
            }

            assert(false); // there must not be any empty buckets
            return TruncatedKey();
        }

        KeyValueResult<Key, Value> get_kv(TruncatedKey const x, Key const bucket_num) const {
            assert(values_.contains(x));
            return { true, bucket_num * sampling_ + x, values_.find(x)->second };
        }
    };

    //ankerl::unordered_dense::map<Key, Bucket*> buckets_;
    std::unique_ptr<Bucket*[]> buckets_;
    size_t num_buckets_;

    Key max_bucket_num_;

public:
    DynamicUniverseSampling(Key const universe) {
        num_buckets_ = idiv_ceil(universe, sampling_);
        buckets_ = std::make_unique<Bucket*[]>(num_buckets_);
        for(size_t i = 0; i < num_buckets_; i++) buckets_[i] = nullptr;
        max_bucket_num_ = 0;
    }

    ~DynamicUniverseSampling() {
        clear();
    }

    void clear() {
        for(size_t i = 0; i < num_buckets_; i++) {
            if(buckets_[i]) {
                delete buckets_[i];
                buckets_[i] = nullptr;
            }
        }
        max_bucket_num_ = 0;
    }

    void insert(Key const key, Value const value) {
        auto const bucket_num = bucket_for(key);
        assert(bucket_num < num_buckets_);

        Bucket* bucket = buckets_[bucket_num];
        if(!bucket) {
            bucket = new Bucket();
            buckets_[bucket_num] = bucket;
            max_bucket_num_ = std::max(bucket_num, max_bucket_num_);
        }

        auto const tkey = truncate(key);
        bucket->bits()[tkey] = 1;
        bucket->values_.emplace(tkey, value);
    }

    bool remove(Key const key) {
        auto const bucket_num = bucket_for(key);
        assert(bucket_num < num_buckets_);

        Bucket* bucket = buckets_[bucket_num];
        if(bucket) [[likely]] {
            auto const tkey = truncate(key);

            auto bits = bucket->bits();
            bool const result = bits[tkey];
            if(result) [[likely]] {
                if(bucket->values_.size() == 1) {
                    // delete bucket completely
                    delete bucket;
                    buckets_[bucket_num] = nullptr;
                    // TODO: update max_bucket_num_?
                } else {
                    // only delete key
                    bits[tkey] = 0;
                    bucket->values_.erase(tkey);
                }
            }
            return result;
        } else {
            return false;
        }
    }

    KeyValueResult<Key, Value> predecessor(Key const key) const {
        auto bucket_num = bucket_for(key);
        assert(bucket_num < num_buckets_);

        auto const tkey = truncate(key);

        // find predecessor in corresponding bucket
        {
            Bucket const* bucket = buckets_[bucket_num];
            if(bucket) {
                // we found the right bucket, attempt to find the predecessor in there
                auto r = bucket->predecessor(tkey);
                if(r.exists) {
                    return bucket->get_kv(r.pos, bucket_num);
                }
            }
        }
        
        // we either couldn't find the right bucket, or the predecessor was not in there
        // find the preceding bucket and return its maximum
        while(bucket_num) {
            --bucket_num;
            Bucket const* bucket = buckets_[bucket_num];
            if(bucket) {
                return bucket->get_kv(bucket->max(), bucket_num);
            }
        }

        // nope
        return KeyValueResult<Key, Value>::none();
    }

    KeyValueResult<Key, Value> successor(Key const key) const {
        auto bucket_num = bucket_for(key);
        assert(bucket_num < num_buckets_);

        auto const tkey = truncate(key);

        // find successor in corresponding bucket
        {
            Bucket const* bucket = buckets_[bucket_num];
            if(bucket) {
                // we found the right bucket, attempt to find the predecessor in there
                auto r = bucket->successor(tkey);
                if(r.exists) {
                    return bucket->get_kv(r.pos, bucket_num);
                }
            }
        }
        
        // we either couldn't find the right bucket, or the successor was not in there
        // find the succeeding bucket and return its minimum
        while(bucket_num < max_bucket_num_) {
            ++bucket_num;
            Bucket const* bucket = buckets_[bucket_num];
            if(bucket) {
                return bucket->get_kv(bucket->min(), bucket_num);
            }
        }

        // nope
        return KeyValueResult<Key, Value>::none();
    }

    bool contains(Key const key) const {
        auto bucket_num = bucket_for(key);
        assert(bucket_num < num_buckets_);

        Bucket const* bucket = buckets_[bucket_num];
        if(bucket) {
            auto bits = bucket->bits();
            return bits[truncate(key)];
        } else {
            return false;
        }
    }

    size_t memory_size() const {
        size_t mem = 0;
        mem += num_buckets_ * sizeof(Bucket*);
        for(size_t i = 0; i < num_buckets_; i++) {
            Bucket const* bucket = buckets_[i];
            if(bucket) {
                mem += sizeof(Bucket);
                mem += packs_per_bucket_ * sizeof(uint64_t);
                mem += memory_size_of(bucket->values_);
            }
        }
        return mem;
    }
};
