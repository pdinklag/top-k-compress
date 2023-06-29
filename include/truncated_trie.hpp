#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "always_inline.hpp"
#include "trie_concepts.hpp"

#include <ankerl/unordered_dense.h>

// a trie where we cut off the topmost levels
template<trie_node Node, size_t cut_>
class TruncatedTrie {
private:
    using Character = typename Node::Character;
    using NodeIndex = typename Node::Index;

    static_assert(cut_ > 1, "please just use a trie");
    static_assert(cut_ <= 8, "we use 64-bit words to pack a cut-off label");
    static_assert(sizeof(Character) == 1, "only 8-bit characters are supported");

    using RootEdgeLabel = uint64_t;

    static RootEdgeLabel pack(Character const* str) {
        RootEdgeLabel packed = 0;
        Character* label = (Character*)&packed;
        for(size_t i = 0; i < cut_; i++) {
            *label++ = *str++;
        }
        return packed;
    }

    static void unpack(RootEdgeLabel const packed, Character* out) {
        Character const* label = (Character const*)&packed;
        for(size_t i = 0; i < cut_; i++) {
            *out++ = *label++;
        }
    }

    static void unpack_reverse(RootEdgeLabel const packed, Character* out) {
        Character const* label = (Character const*)&packed + cut_ - 1;
        for(size_t i = 0; i < cut_; i++) {
            *out++ = *label--;
        }
    }

    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;
    ankerl::unordered_dense::map<RootEdgeLabel, NodeIndex> root_children_;
    ankerl::unordered_dense::map<NodeIndex, RootEdgeLabel> root_labels_;

    #ifndef NDEBUG
    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        return nodes_[parent].children.contains(node);
    }
    #endif

public:
    TruncatedTrie(NodeIndex const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(NodeIndex i = 0; i < capacity_; i++) {
            nodes_[i] = Node();
        }
    }

    void insert_root_child(NodeIndex const node, Character const* label) {
        assert(node < capacity_);
        assert(!root_children_.contains(pack(label)));

        nodes_[node] = Node(root(), label[cut_ - 1]);

        auto const packed_label = pack(label);
        root_children_.emplace(packed_label, node);
        root_labels_.emplace(node, packed_label);

        assert(is_leaf(node));
    }

    void insert_child(NodeIndex const node, NodeIndex const parent, Character const label) {
        assert(parent != root()); // we cannot create a root child like this
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
        if(parent == root()) {
            // we are removing a root child
            assert(root_labels_.contains(node));
            auto const packed_label = root_labels_.find(node)->second;

            assert(root_children_.contains(packed_label));
            assert(root_children_.find(packed_label)->second == node);

            root_labels_.erase(node);
            root_children_.erase(packed_label);
        } else {
            // we are removing a regular child
            assert(is_child_of(node, parent));

            auto const label = nodes_[node].inlabel;
            nodes_[parent].children.remove(label);
            assert(!is_child_of(node, parent));
        }

        return parent;
    }

    NodeIndex new_node() {
        return size_++;
    }

    bool try_get_root_child(Character const* label, NodeIndex& out_child) const ALWAYS_INLINE {
        auto const packed_label = pack(label);
        auto it = root_children_.find(packed_label);
        if(it != root_children_.end()) {
            out_child = it->second;
            return true;
        } else {
            return false;
        }
    }

    bool try_get_child(NodeIndex const node, Character const label, NodeIndex& out_child) const ALWAYS_INLINE {
        assert(node != root()); // we cannot navigate from the root like this
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

    size_t spell_reverse(NodeIndex const node, Character* buffer) const {
        if(node == root()) return 0;

        size_t d = 0;
        auto v = node;
        while(nodes_[v].parent) {
            buffer[d++] = nodes_[v].inlabel;
            v = nodes_[v].parent;
        }

        assert(root_labels_.contains(v));
        auto const packed_label = root_labels_.find(v)->second;
        unpack_reverse(packed_label, buffer + d);
        return d + cut_;
    }

    size_t spell(NodeIndex const node, Character* buffer) const {
        auto const d = spell_reverse(node, buffer);
        std::reverse(buffer, buffer + d);
        return d;
    }
};
