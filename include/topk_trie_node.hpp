#pragma once

#include "min_pq.hpp"
#include "trie_node.hpp"

struct TopkTrieNodeData {
    size_t freq;
    size_t insert_freq;
    uint64_t fingerprint;
    MinPQ<size_t>::Location minpq;
} __attribute__((packed));

template<std::unsigned_integral NodeIndex = uint32_t>
struct TopkTrieNode : public TrieNode<NodeIndex>, public TopkTrieNodeData {
    using TrieNode<NodeIndex>::Character;
    using TrieNode<NodeIndex>::Index;
    using TrieNode<NodeIndex>::TrieNode;
} __attribute__((packed));
