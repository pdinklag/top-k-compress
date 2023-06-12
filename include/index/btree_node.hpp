#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

template<std::totally_ordered Key, typename Value, size_t capacity_>
class BTreeSortedNodeLS {
public:
    struct PosResult {
        bool exists;
        size_t pos;
        
        inline operator bool() const { return exists; }
        inline operator size_t() const { return pos; }
    };

private:
    static_assert(capacity_ < 65536);
    using Size = typename std::conditional_t<capacity_ < 256, uint8_t, uint16_t>;

    Key keys_[capacity_];
    Value values_[capacity_];
    Size size_;

public:
    BTreeSortedNodeLS(): size_(0) {
    }

    BTreeSortedNodeLS(const BTreeSortedNodeLS&) = default;
    BTreeSortedNodeLS(BTreeSortedNodeLS&&) = default;

    BTreeSortedNodeLS& operator=(const BTreeSortedNodeLS&) = default;
    BTreeSortedNodeLS& operator=(BTreeSortedNodeLS&&) = default;

    inline Key operator[](size_t const i) const {
        return keys_[i];
    }

    inline Value value(size_t const i) const {
        return values_[i];
    }

    PosResult predecessor(Key const x) const {
        if(size_ == 0) [[unlikely]] return { false, 0 };
        if(x < keys_[0]) [[unlikely]] return { false, 0 };
        if(x >= keys_[size_-1]) [[unlikely]] return { true, size_ - 1ULL };
        
        size_t i = 1;
        while(keys_[i] <= x) ++i;
        return { true, i-1 };
    }

    PosResult successor(Key const x) const {
        if(size_ == 0) [[unlikely]] return { false, 0 };
        if(x <= keys_[0]) [[unlikely]] return { true, 0 };
        if(x > keys_[size_-1]) [[unlikely]] return { false, 0 };
        
        size_t i = 1;
        while(keys_[i] < x) ++i;
        return { true, i };
    }

    void insert(Key const key, Value const value) {
        assert(size_ < capacity_);
        size_t i = 0;
        while(i < size_ && keys_[i] < key) ++i;
        for(size_t j = size_; j > i; j--) keys_[j] = keys_[j-1];
        keys_[i] = key;
        for(size_t j = size_; j > i; j--) values_[j] = values_[j-1];
        values_[i] = value;
        ++size_;
    }

    bool remove(Key const key, Value& value) {
        assert(size_ > 0);
        for(size_t i = 0; i < size_; i++) {
            if(keys_[i] == key) {
                value = values_[i];
                for(size_t j = i; j < size_-1; j++) keys_[j] = keys_[j+1];
                for(size_t j = i; j < size_-1; j++) values_[j] = values_[j+1];
                --size_;
                return true;
            }
        }
        return false;
    }

    bool remove(Key const key) {
        Value discard;
        return remove(key, discard);
    }

    inline size_t size() const { return size_; }
} __attribute__((__packed__));
