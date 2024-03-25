#pragma once

#include <concepts>
#include <memory>
#include <stack>
#include <vector>

#include "trie_edge_array.hpp"

template<std::unsigned_integral NodeIndex = size_t>
class SimpleTrie {
public:
    using Node = NodeIndex;
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

    NodeData const& node(Node const i) const {
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

    template<typename Trie>
    void construct(Trie const& other, size_t const other_v, Node const v) {
        auto const& children = other.children_of(other_v);
        for(size_t i = 0; i < children.size(); i++) {
            Node child;
            follow_edge(v, children.label(i), child);
            construct(other, children[i], child);
        }
    }

public:
    SimpleTrie() {
        clear();
    }

    SimpleTrie(SimpleTrie&&) = default;
    SimpleTrie& operator=(SimpleTrie&&) = default;

    SimpleTrie(SimpleTrie const&) = delete;
    SimpleTrie& operator=(SimpleTrie const&) = delete;


    template<typename Trie>
    SimpleTrie(Trie const& other) {
        clear();
        construct(other, other.root(), root());
    }

    SimpleTrie(std::vector<bool> const& topology, std::string const& labels) {
        clear();

        assert(topology[0] == true);

        auto const last = topology.size() - 1;
        assert(topology[last] == false);

        Node v = root();
        std::stack<Node> stack;
        size_t num_nodes = 1;

        for(size_t i = 1; i < last; i++) {
            if(topology[i]) {
                // open
                stack.push(v);

                auto const b = follow_edge(v, labels[num_nodes++], v);
                assert(!b);
                assert(num_nodes == size_);
            } else {
                // close
                assert(!stack.empty());
                v = stack.top();
                stack.pop();
            }
        }
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

    bool try_get_child(Node const v, char const c, Node& out_node) const {
        return node(v).try_get(c, out_node);
    }

    void clear() {
        size_ = 1;
        capacity_ = block_size_;
        blocks_.clear();
        blocks_.emplace_back(alloc_block());
        node(0) = NodeData();
    }

    auto const& children_of(Node const v) const { return node(v); }

    void print_debug_info() const {
    }

    size_t mem_size() const {
        return sizeof(SimpleTrie) + (capacity_ / block_size_) * sizeof(std::unique_ptr<NodeData[]>) + capacity_ * sizeof(NodeData);
    }
};
