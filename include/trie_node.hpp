#pragma once

#include <cstdint>

#include "always_inline.hpp"
#include "trie_edge_array.hpp"

template<std::unsigned_integral NodeIndex = uint32_t>
struct TrieNode {
    using Character = char;
    using Index = NodeIndex;
    using Size = uint16_t;

    using ChildArray = TrieEdgeArray<Character, Index, Size>;

    ChildArray children;
    Character inlabel;
    Index parent;

    TrieNode(Index const _parent, Character const _inlabel) : parent(_parent), inlabel(_inlabel) {
    }
    
    TrieNode() : TrieNode(0, 0) {
    }

    size_t size() const ALWAYS_INLINE {
        return children.size();
    }

    bool is_leaf() const ALWAYS_INLINE {
        return size() == 0;
    }
} __attribute__((packed));
