#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include <idiv_ceil.hpp>

#include "result.hpp"

template<std::unsigned_integral Key, std::unsigned_integral Value, size_t sample_bits_>
class DynamicUniverseSampling {
private:
    static constexpr size_t key_bits_ = std::numeric_limits<Key>::digits;
    static_assert(key_bits_ > sample_bits_);
    static constexpr size_t trunc_bits_ = key_bits_ - sample_bits_;
    static_assert(trunc_bits_ <= 32);
    static constexpr size_t max_bucket_size_ = 1ULL << sample_bits_;
    static constexpr Key trunc_mask_ = Key(max_bucket_size_ - 1);

    static constexpr Key NONE = -1;

    using TruncatedKey = std::conditional_t<trunc_bits_ <=8, uint8_t,std::conditional_t<trunc_bits_ <= 16, uint16_t, uint32_t>>;

    struct Bucket {
        std::vector<TruncatedKey> keys;
        std::vector<Value> values;

        size_t size() const { return keys.size(); }

        bool contains(TruncatedKey const trunc_key) const {
            for(size_t i = 0; i < size(); i++) {
                if(keys[i] == trunc_key) return true;
            }
            return false;
        }

        KeyValueResult<Key, Value> max() const {
            assert(size() > 0);

            Key max_idx = 0;
            TruncatedKey max = keys[0];
            for(size_t i = 1; i < size(); i++) {
                auto const x = keys[i];
                if(x > max) {
                    max_idx = i;
                    max = x;
                }
            }
            return { true, max, values[max_idx] };
        }

        KeyValueResult<Key, Value> min() const {
            assert(size() > 0);

            Key min_idx = 0;
            TruncatedKey min = keys[0];
            for(size_t i = 1; i < size(); i++) {
                auto const x = keys[i];
                if(x < min) {
                    min_idx = i;
                    min = x;
                }
            }
            return { true, min, values[min_idx] };
        }

        KeyValueResult<Key, Value> predecessor(TruncatedKey const trunc_key) const {
            assert(size() > 0);

            Key pred_idx = NONE;
            TruncatedKey pred;
            for(size_t i = 0; i < size(); i++) {
                auto const x = keys[i];
                if(x <= trunc_key && (pred_idx == NONE || x > pred)) {
                    pred_idx = i;
                    pred = x;
                    if(x == trunc_key) break;
                }
            }
            return (pred_idx != NONE) ? KeyValueResult<Key, Value>{ true, pred, values[pred_idx]} : KeyValueResult<Key, Value>::none();
        }

        KeyValueResult<Key, Value> successor(TruncatedKey const trunc_key) const {
            assert(size() > 0);

            Key succ_idx = NONE;
            TruncatedKey succ;
            for(size_t i = 0; i < size(); i++) {
                auto const x = keys[i];
                if(x >= trunc_key && (succ_idx == NONE || x < succ)) {
                    succ_idx = i;
                    succ = x;
                    if(x == trunc_key) break;
                }
            }
            return (succ_idx != NONE) ? KeyValueResult<Key, Value>{ true, succ, values[succ_idx]} : KeyValueResult<Key, Value>::none();
        }
    };

    inline static KeyValueResult<Key, Value> reconstruct_key(KeyValueResult<Key, Value>&& kvr, size_t const bucket_num) {
        return KeyValueResult<Key, Value> { kvr.exists, Key(bucket_num * max_bucket_size_) + kvr.key, kvr.value };
    }

    size_t max_buckets_;
    std::unique_ptr<Bucket*[]> buckets_;
    std::unique_ptr<Key[]> active_pred_;
    std::unique_ptr<Key[]> active_succ_;

public:
    DynamicUniverseSampling(size_t const universe) {
        max_buckets_ = idiv_ceil(universe, max_bucket_size_);

        buckets_ = std::make_unique<Bucket*[]>(max_buckets_);
        active_pred_ = std::make_unique<Key[]>(max_buckets_);
        active_succ_ = std::make_unique<Key[]>(max_buckets_);
        for(size_t i = 0; i < max_buckets_; i++) {
            buckets_[i] = nullptr;
            active_pred_[i] = NONE;
            active_succ_[i] = NONE;
        }
    }

    ~DynamicUniverseSampling() {
        for(size_t i = 0; i < max_buckets_; i++) {
            if(buckets_[i] != nullptr) delete buckets_[i];
        }
    }

    void clear() {
        for(size_t i = 0; i < max_buckets_; i++) {
            if(buckets_[i] != nullptr) delete buckets_[i];

            buckets_[i] = nullptr;
            active_pred_[i] = NONE;
            active_succ_[i] = NONE;
        }
    }    

    void insert(Key const key, Value const value) {
        auto const bucket_num = key >> sample_bits_;
        assert(bucket_num < max_buckets_);

        Bucket* bucket = buckets_[bucket_num];
        if(bucket == nullptr) {
            bucket = new Bucket();
            buckets_[bucket_num] = bucket;

            // update active
            {
                size_t i = bucket_num;
                while(i < max_buckets_ && (active_pred_[i] == NONE || active_pred_[i] < bucket_num)) {
                    active_pred_[i] = bucket_num;
                    ++i;
                }

                size_t j = bucket_num + 1;
                while(j > 0 && (active_succ_[j-1] == NONE || active_succ_[j-1] > bucket_num)) {
                    active_succ_[j-1] = bucket_num;
                    --j;
                }
            }   
        }

        bucket->values.emplace_back(std::move(value));

        auto const trunc_key = key & trunc_mask_;
        bucket->keys.emplace_back(trunc_key);
    }

    bool remove(Key const key) {
        auto const bucket_num = key >> sample_bits_;
        assert(bucket_num < max_buckets_);

        bool removed = false;
        Bucket* bucket = buckets_[bucket_num];
        if(bucket != nullptr) {
            auto const trunc_key = key & trunc_mask_;
            for(size_t i = 0; i < bucket->size(); i++) {
                if(bucket->keys[i] == trunc_key) {
                    // swap with last, then pop
                    std::swap(bucket->keys[i], bucket->keys.back());
                    bucket->keys.pop_back();
                    std::swap(bucket->values[i], bucket->values.back());
                    bucket->values.pop_back();
                    removed = true;
                    break;
                }
            }

            if(removed && bucket->size() == 0) {
                // delete empty bucket
                delete bucket;
                buckets_[bucket_num] = nullptr;

                // update active
                auto const prev_active_bucket_num = (bucket_num > 0) ? active_pred_[bucket_num - 1] : NONE;
                for(size_t i = bucket_num; i < max_buckets_ && active_pred_[i] == bucket_num; i++) {
                    active_pred_[i] = prev_active_bucket_num;
                }

                auto const next_active_bucket_num = (bucket_num + 1 < max_buckets_) ? active_succ_[bucket_num + 1] : NONE;
                for(size_t j = bucket_num + 1; j > 0 && active_succ_[j-1] == bucket_num; j--) {
                    active_succ_[j-1] = next_active_bucket_num;
                }
            }
        }
        return removed;
    }

    bool contains(Key const key) const {
        auto const bucket_num = key >> sample_bits_;
        assert(bucket_num < max_buckets_);

        auto const active_bucket_num = active_pred_[bucket_num];
        if(active_bucket_num == bucket_num) {
            return buckets_[bucket_num]->contains(key & trunc_mask_);
        } else {
            return false;
        }
    }

    KeyValueResult<Key, Value> predecessor(Key const key) const {
        auto const bucket_num = key >> sample_bits_;
        assert(bucket_num < max_buckets_);

        auto const active_bucket_num = active_pred_[bucket_num];
        if(active_bucket_num != NONE){
            if(active_bucket_num < bucket_num) {
                // the bucket that the key would reside in doesn't exist
                // we simply report the maximum of the previous active bucket
                return reconstruct_key(buckets_[active_bucket_num]->max(), active_bucket_num);
            } else {
                assert(active_bucket_num == bucket_num);

                // find predecessor in matching bucket
                auto result = reconstruct_key(buckets_[bucket_num]->predecessor(key & trunc_mask_), bucket_num);
                if(result.exists) {
                    return result;
                } else if(active_bucket_num > 0) {
                    // we couldn't find a predecessor in the matching bucket, so the predecessor must be the maximum of the previous active bucket
                    auto const prev_active_bucket_num = active_pred_[active_bucket_num - 1];
                    if(prev_active_bucket_num != NONE) {
                        return reconstruct_key(buckets_[prev_active_bucket_num]->max(), prev_active_bucket_num);
                    }
                }
            }
        }

        // there is no predecessor
        return KeyValueResult<Key, Value>::none();
    }

    KeyValueResult<Key, Value> successor(Key const key) const {
        auto const bucket_num = key >> sample_bits_;
        assert(bucket_num < max_buckets_);

        auto const active_bucket_num = active_succ_[bucket_num];
        if(active_bucket_num != NONE){
            if(active_bucket_num > bucket_num) {
                // the bucket that the key would reside in doesn't exist
                // we simply report the minimum of the next active bucket
                return reconstruct_key(buckets_[active_bucket_num]->min(), active_bucket_num);
            } else {
                assert(active_bucket_num == bucket_num);

                // find successor in matching bucket
                auto result = reconstruct_key(buckets_[bucket_num]->successor(key & trunc_mask_), bucket_num);
                if(result.exists) {
                    return result;
                } else if(active_bucket_num + 1 < max_buckets_) {
                    // we couldn't find a successor in the matching bucket, so the successor must be the minimum of the next active bucket
                    auto const next_active_bucket_num = active_succ_[active_bucket_num + 1];
                    if(next_active_bucket_num != NONE) {
                        return reconstruct_key(buckets_[next_active_bucket_num]->min(), next_active_bucket_num);
                    }
                }
            }
        }

        // there is no successor
        return KeyValueResult<Key, Value>::none();
    }
};
