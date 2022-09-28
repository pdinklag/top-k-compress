#pragma once

#include "min_pq.hpp"
#include "trie_node.hpp"

template<std::unsigned_integral NodeIndex = uint32_t>
struct TopkTrieNode : public TrieNode<NodeIndex> {
    using TrieNode<NodeIndex>::Character;
    using TrieNode<NodeIndex>::Index;

    size_t freq;
    size_t insert_freq;
    uint64_t fingerprint;
    MinPQ<size_t>::Location minpq;

    using TrieNode<NodeIndex>::TrieNode;

} __attribute__((packed));
