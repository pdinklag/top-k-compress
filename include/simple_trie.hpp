#pragma once

#include <memory>
#include <vector>

#include "trie_edge_array.hpp"

class SimpleTrie {
public:
    using Node = size_t;
    using NodeData = TrieEdgeArray<char, Node>;

private:
    static constexpr size_t block_size_ = 1ULL << 20;
    static constexpr size_t block_mask_ = block_size_ - 1;

    std::vector<std::unique_ptr<NodeData[]>> blocks_;
    size_t size_;
    size_t capacity_;

    NodeData& node(Node const i) {
        auto const block = i / block_size_;
        return blocks_[block][i & block_mask_];
    }

    Node insert_child(Node const parent, char const c) {
        auto const v = size_++;
        if(v == capacity_) {
            blocks_.emplace_back(alloc_block());
            capacity_ += block_size_;
        }

        node(v) = NodeData();
        node(parent).insert(c, v);
        return v;
    }

    std::unique_ptr<NodeData[]> alloc_block() {
        return std::make_unique<NodeData[]>(block_size_);
    }

public:
    SimpleTrie() {
        clear();
    }

    Node root() const { return 0; }

    size_t size() const { return size_; }

    bool follow_edge(Node const v, char const c, Node& out_node) {
        auto const found = node(v).try_get(c, out_node);
        if(!found) {
            out_node = insert_child(v, c);
        }
        return found;
    }

    void clear() {
        size_ = 1;
        capacity_ = block_size_;
        blocks_.clear();
        blocks_.emplace_back(alloc_block());
        node(0) = NodeData();
    }
};
