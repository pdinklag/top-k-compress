#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "always_inline.hpp"

// mantains an array of trie edges
template<std::integral Character = char, std::unsigned_integral NodeIndex = uint32_t, std::unsigned_integral Size = uint16_t>
class TrieEdgeArray {
private:
    static constexpr size_t capacity_for(size_t const n) {
        return (n > 0) ? std::bit_ceil(n) : 0;
    }

    using UCharacter = std::make_unsigned_t<Character>;
    using BitPack = uintmax_t;

    static constexpr size_t bits_per_pack_ = 8 * sizeof(BitPack);
    static constexpr size_t bit_pack_mask_ = bits_per_pack_ - 1;
    static constexpr size_t sigma_ = ((size_t)std::numeric_limits<UCharacter>::max() + 1);
    static_assert((sigma_ % bits_per_pack_) == 0);
    static constexpr size_t num_bit_packs_ = sigma_ / bits_per_pack_;

    struct ExternalArray {
        BitPack ind[num_bit_packs_];
        NodeIndex* links;

        #ifndef NDEBUG
        // only used for debugging
        size_t size() const ALWAYS_INLINE {
            size_t s = 0;
            for(size_t i = 0; i < num_bit_packs_; i++) {
                s += std::popcount(ind[i]);
            }
            return s;
        }
        #endif

        void clear() ALWAYS_INLINE {
            for(size_t i = 0; i < num_bit_packs_; i++) {
                ind[i] = 0;
            }
        }

        void set(UCharacter const i) ALWAYS_INLINE {
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            ind[b] |= (1ULL << j);
        }

        void unset(UCharacter const i) ALWAYS_INLINE {
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            ind[b] &= ~(1ULL << j);
        }

        bool get(UCharacter const i) const ALWAYS_INLINE {
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            return (ind[b] & (1ULL << j)) != 0;
        }
        
        size_t rank(UCharacter const i) const ALWAYS_INLINE {
            assert(get(i));

            size_t r = 0;
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            for(size_t i = 0; i < b; i++) {
                r += std::popcount(ind[i]);
            }

            BitPack const mask = std::numeric_limits<BitPack>::max() >> (std::numeric_limits<BitPack>::digits - 1 - j);
            return r + std::popcount(ind[b] & mask) - 1;
        }
    } __attribute__((packed));

public:
    static constexpr size_t inline_size_ = sizeof(ExternalArray) / (sizeof(NodeIndex) + sizeof(Character));
    static constexpr size_t inline_align_ = sizeof(ExternalArray) - inline_size_ * (sizeof(NodeIndex) + sizeof(Character));

private:
    struct InlineArray {
        Character labels[inline_size_];
        NodeIndex links[inline_size_];
    } __attribute__((packed));

    Size size_;
    union {
        ExternalArray ext;
        InlineArray   inl;
    } data_;

    size_t find(Character const label) const ALWAYS_INLINE {
        if(is_inline()) {
            NodeIndex found = 0;
            while(found < size_ && data_.inl.labels[found] != label) ++found;
            return found;
        } else {
            return data_.ext.get(label) ? data_.ext.rank(label) : size_;
        }
    }

public:
    TrieEdgeArray() {
        data_.ext.clear();
        data_.ext.links = nullptr;
    }

    ~TrieEdgeArray() {
        if(!is_inline()) {
            delete[] data_.ext.links;
        }
    }

    bool is_inline() const ALWAYS_INLINE {
        return size_ <= inline_size_;
    }

    Size size() const ALWAYS_INLINE {
        return size_;
    }

    bool contains(NodeIndex const what) const ALWAYS_INLINE {
        if(is_inline()) {
            for(NodeIndex i = 0; i < size_; i++) {
                if(data_.inl.links[i] == what) {
                    return true;
                }
            }
        } else {
            for(NodeIndex i = 0; i < size_; i++) {
                if(data_.ext.links[i] == what) {
                    return true;
                }
            }
        }
        return false;
    }

    void insert(Character const label, NodeIndex const link) {
        // possibly allocate slots
        // nb: because we are only keeping track of the size and assume the capacity to always be its hyperceil,
        //     we may re-allocate here even though it would not be necessary
        //     however, if there were many removals, this may also (unknowingly) actually shrink the capacity
        //     in any event: it's not a bug, it's a feature!
        if(size_ == inline_size_ || (!is_inline() && size_ == capacity_for(size_))) {
            auto* new_links = new NodeIndex[capacity_for(size_ + 1)];
            if(is_inline()) {
                assert(size_ == inline_size_);

                // becoming large
                Character old_labels[inline_size_];

                // we need to make sure now that the children are transferred in the order of their labels
                std::pair<UCharacter, NodeIndex> old_child_info[inline_size_];
                for(size_t j = 0; j < size_; j++) {
                    auto const child_j = data_.inl.links[j];
                    old_child_info[j] = { data_.inl.labels[j], child_j };
                }
                std::sort(old_child_info, old_child_info + size_, [](auto const& a, auto const& b){ return a.first < b.first; });

                // clear all bits first
                data_.ext.clear();

                // set bits that need to be set and write children
                for(size_t j = 0; j < size_; j++) {
                    data_.ext.set(old_child_info[j].first);
                    new_links[j] = old_child_info[j].second;
                }
            } else {
                // staying large
                std::copy(data_.ext.links, data_.ext.links + size_, new_links);
                delete[] data_.ext.links;
            }
            data_.ext.links = new_links;
        }
        
        // insert
        ++size_;
        if(is_inline()) {
            auto const i = size_ - 1;
            data_.inl.labels[i] = label;
            data_.inl.links[i] = link;
        } else {
            data_.ext.set(label);
            assert(size_ == data_.ext.size());

            auto const i = data_.ext.rank(label);
            for(size_t j = size_ - 1; j > i; j--) {
                data_.ext.links[j] = data_.ext.links[j - 1];
            }
            data_.ext.links[i] = link;
        }

        assert(contains(link));
    }

    void remove(Character const label) {
        auto const i = find(label);
        if(i < size_) {
            // remove from link array if necessary
            if(is_inline()) {
                // swap with last child if necessary
                if(size_ > 1) {
                    NodeIndex const last = size_ - 1;
                    data_.inl.labels[i] = data_.inl.labels[last];
                    data_.inl.links[i] = data_.inl.links[last];
                }
            } else {
                // remove
                data_.ext.unset(label);
                assert(data_.ext.size() == size_ - 1);

                if(size_ > 1) {
                    for(size_t j = i; j + 1 < size_; j++) {
                        data_.ext.links[j] = data_.ext.links[j+1];
                    }
                }
            }

            // possibly convert into inline node
            auto const was_inline = is_inline();
            --size_; // nb: must be done before moving data

            if(!was_inline && is_inline()) {
                assert(size_ == inline_size_);
                Character new_labels[inline_size_];
                NodeIndex new_links[inline_size_];

                {
                    size_t j = 0;
                    for(size_t c = 0; c <= std::numeric_limits<UCharacter>::max(); c++) {
                        if(data_.ext.get(c)) {
                            new_labels[j] = (Character)c;
                            new_links[j] = data_.ext.links[j];
                            ++j;
                        }
                    }
                    assert(j == inline_size_);
                }

                delete[] data_.ext.links;

                for(size_t j = 0; j < size_; j++) {
                    data_.inl.labels[j] = new_labels[j];
                    data_.inl.links[j] = new_links[j];
                }
            }
        } else {
            // "this kid is not my son"
            assert(false);
        }
    }

    bool try_get(Character const label, NodeIndex& out_link) const ALWAYS_INLINE {
        if(is_inline()) {
            for(NodeIndex i = 0; i < size_; i++) {
                if(data_.inl.labels[i] == label) {
                    out_link = data_.inl.links[i];
                    return true;
                }
            }
            return false;
        } else {
            if(data_.ext.get(label)) {
                out_link = data_.ext.links[data_.ext.rank(label)];
                return true;
            } else {
                return false;
            }
        }
    }

} __attribute__((packed));
