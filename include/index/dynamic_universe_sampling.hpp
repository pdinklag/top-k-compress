#pragma once

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

    struct Bucket {
        std::unique_ptr<uint64_t[]> bits_;
        ankerl::unordered_dense::map<TruncatedKey, Value> values_;

        Bucket() {
            bits_ = std::make_unique<uint64_t[]>(packs_per_bucket_);
            for(size_t i = 0; i < packs_per_bucket_; i++) bits_[i] = 0;
        }

        auto bits() { return word_packing::bit_accessor(bits_.get()); }
        auto bits() const { return word_packing::bit_accessor(bits_.get()); }

        PosResult predecessor(size_t x) const {
            auto bv = bits();
            ++x;
            while(x > 0) {
                if(bv[x-1]) return { true, x-1 };
                else --x;
            }
            return PosResult::none();
        }

        PosResult successor(size_t x) const {
            auto bv = bits();
            while(x < sampling_) {
                if(bv[x]) return { true, x };
                else ++x;
            }
            return PosResult::none();
        }

        TruncatedKey min() const {
            auto bv = bits();
            for(size_t x = 0; x < sampling_; x++) {
                if(bv[x]) return x;
            }
            assert(false); // there must not be any empty buckets
            return TruncatedKey();
        }

        TruncatedKey max() const {
            auto bv = bits();
            for(size_t x = sampling_; x > 0; x--) {
                if(bv[x-1]) return x-1;
            }
            assert(false); // there must not be any empty buckets
            return TruncatedKey();
        }

        KeyValueResult<Key, Value> get_kv(TruncatedKey const x, Key const bucket_num) const {
            assert(values_.contains(x));
            return { true, bucket_num * sampling_ + x, values_.find(x)->second };
        }
    };

    ankerl::unordered_dense::map<Key, Bucket*> buckets_;
    Key max_bucket_num_;

public:
    DynamicUniverseSampling() : max_bucket_num_(0) {
    }

    ~DynamicUniverseSampling() {
        clear();
    }

    void clear() {
        for(auto& e : buckets_) {
            delete e.second;
        }
        buckets_.clear();
        max_bucket_num_ = 0;
    }

    void insert(Key const key, Value const value) {
        Bucket* bucket;

        auto const bucket_num = bucket_for(key);
        auto it = buckets_.find(bucket_num);
        if(it != buckets_.end()) {
            bucket = it->second;
        } else {
            bucket = new Bucket();
            buckets_.emplace(bucket_num, bucket);
            max_bucket_num_ = std::max(bucket_num, max_bucket_num_);
        }

        auto const tkey = truncate(key);
        bucket->bits()[tkey] = 1;
        bucket->values_.emplace(tkey, value);
    }

    bool remove(Key const key) {
        auto const bucket_num = bucket_for(key);
        auto it = buckets_.find(bucket_num);
        if(it != buckets_.end()) [[likely]] {
            Bucket* bucket = it->second;
            auto const tkey = truncate(key);

            auto bits = bucket->bits();
            bool const result = bits[tkey];
            if(result) [[likely]] {
                if(bucket->values_.size() == 1) {
                    // delete bucket completely
                    delete bucket;
                    buckets_.erase(bucket_num);
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
        auto const tkey = truncate(key);

        // find predecessor in corresponding bucket
        {
            auto it = buckets_.find(bucket_num);
            if(it != buckets_.end()) {
                // we found the right bucket, attempt to find the predecessor in there
                Bucket const* bucket = it->second;
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
            auto it = buckets_.find(bucket_num);
            if(it != buckets_.end()) {
                Bucket const* bucket = it->second;
                return bucket->get_kv(bucket->max(), bucket_num);
            }
        }

        // nope
        return KeyValueResult<Key, Value>::none();
    }

    KeyValueResult<Key, Value> successor(Key const key) const {
        auto bucket_num = bucket_for(key);
        auto const tkey = truncate(key);

        // find successor in corresponding bucket
        {
            auto it = buckets_.find(bucket_num);
            if(it != buckets_.end()) {
                // we found the right bucket, attempt to find the predecessor in there
                Bucket const* bucket = it->second;
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
            auto it = buckets_.find(bucket_num);
            if(it != buckets_.end()) {
                Bucket const* bucket = it->second;
                return bucket->get_kv(bucket->min(), bucket_num);
            }
        }

        // nope
        return KeyValueResult<Key, Value>::none();
    }

    bool contains(Key const key) const {
        auto it = buckets_.find(bucket_for(key));
        if(it != buckets_.end()) [[likely]] {
            auto bits = it->second->bits();
            return bits[truncate(key)];
        } else {
            return false;
        }
    }

    size_t memory_size() const {
        size_t mem = 0;
        mem += memory_size_of(buckets_);
        for(auto& e : buckets_) {
            mem += sizeof(Bucket);
            mem += packs_per_bucket_ * sizeof(uint64_t);
            mem += memory_size_of(e.second->values_);
        }
        return mem;
    }
};
