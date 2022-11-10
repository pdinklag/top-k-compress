#pragma once

#include "min_pq.hpp"
#include "trie_node.hpp"

template<std::unsigned_integral NodeIndex = uint32_t>
struct TopkTrieNode : public TrieNode<NodeIndex> {
    using TrieNode<NodeIndex>::Character;
    using TrieNode<NodeIndex>::Index;
    using TrieNode<NodeIndex>::TrieNode;

    size_t freq;                    // the current estimated frequency
    size_t insert_freq;             // the estimated frequency at time of insertion
    uint64_t fingerprint;           // the 64-bit fingerprint of the represented string
    MinPQ<size_t>::Location minpq;  // the entry in the minimum PQ, if any

} __attribute__((packed));
