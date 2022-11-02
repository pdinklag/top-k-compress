#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "always_inline.hpp"

template<typename Node> requires 
    requires {
        typename Node::Character;
        typename Node::Index;
    }
    && std::constructible_from<Node> // default
    && std::constructible_from<Node, typename Node::Index, typename Node::Character> // parent / label
    && requires(Node node) {
        { node.children };
        { node.inlabel };
        { node.parent };
    } && requires(Node const& node) {
        { node.size() } -> std::convertible_to<size_t>;
        { node.is_leaf() } -> std::same_as<bool>;
    }
class Trie {
private:
    using Character = typename Node::Character;
    using NodeIndex = typename Node::Index;

    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;

    #ifndef NDEBUG
    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        return nodes_[parent].children.contains(node);
    }
    #endif

public:
    Trie(NodeIndex const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(NodeIndex i = 0; i < capacity_; i++) {
            nodes_[i] = Node();
        }
    }

    void insert_child(NodeIndex const node, NodeIndex const parent, Character const label) {
        assert(node < capacity_);

        NodeIndex discard;
        assert(!try_get_child(parent, label, discard));

        nodes_[parent].children.insert(label, node);
        
        assert(nodes_[node].is_leaf()); // even though there is no semantical reason, let's make sure we are not leaking a children array
        nodes_[node] = Node(parent, label);

        assert(is_leaf(node));
    }

    // extract node from trie and return parent
    NodeIndex extract(NodeIndex const node) {
        assert(is_leaf(node)); // cannot extract an inner node

        auto const parent = nodes_[node].parent;
        assert(is_child_of(node, parent));
        
        auto const label = nodes_[node].inlabel;
        nodes_[parent].children.remove(label);
        assert(!is_child_of(node, parent));

        return parent;
    }

    NodeIndex new_node() {
        return size_++;
    }

    bool try_get_child(NodeIndex const node, Character const label, NodeIndex& out_child) ALWAYS_INLINE {
        return nodes_[node].children.try_get(label, out_child);
    }

    bool is_leaf(NodeIndex const node) const {
        return nodes_[node].is_leaf();
    }

    NodeIndex root() const {
        return 0;
    }

    NodeIndex parent(NodeIndex const node) const {
        return nodes_[node].parent;
    }

    bool full() const { 
        return size_ == capacity_;
    }

    Node& node(NodeIndex const v) {
        return nodes_[v];
    }

    Node const& node(NodeIndex const v) const {
        return nodes_[v];
    }

    size_t spell(NodeIndex const node, Character* buffer) const {
        // spell reverse
        size_t d = 0;
        auto v = node;
        while(v) {
            buffer[d++] = nodes_[v].inlabel;
            v = nodes_[v].parent;
        }

        // reverse and return length
        std::reverse(buffer, buffer + d);
        return d;
    }

    void print_debug_info() const {
        size_t num_leaves = 0;
        size_t num_small = 0;
        
        for(size_t i = 0; i < capacity_; i++) {
            auto& v = nodes_[i];

            if(v.size() == 0) ++num_leaves;
            if(v.size() <= Node::ChildArray::inline_size_) ++num_small;
        }

        std::cout << "trie info"
                  << ": sizeof(Node)=" << sizeof(Node)
                  << ", small_node_size_=" << Node::ChildArray::inline_size_
                  << ", small_node_align_=" << Node::ChildArray::inline_align_
                  << ", num_leaves=" << num_leaves
                  << ", num_small=" << num_small
                  << std::endl;
    }
};
