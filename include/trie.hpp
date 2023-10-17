#pragma once

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>

#include "always_inline.hpp"
#include "trie_concepts.hpp"

template<trie_node Node, bool allow_orphans_ = false>
class Trie {
private:
    using Character = typename Node::Character;
    using NodeIndex = typename Node::Index;

public:
    static constexpr NodeIndex ROOT = 0;
    static constexpr NodeIndex NIL = std::numeric_limits<NodeIndex>::max(); // nb: used to denote orphans and is only ever used if orphans are allowed

    static constexpr bool is_nil(NodeIndex const x) ALWAYS_INLINE { return x == NIL; }
    static constexpr bool is_valid(NodeIndex const x) ALWAYS_INLINE { return !is_nil(x); }
    static constexpr bool is_root(NodeIndex const x) ALWAYS_INLINE { return x == ROOT; }
    static constexpr bool is_root_or_nil(NodeIndex const x) ALWAYS_INLINE { return is_root(x) || is_nil(x); }
    static constexpr bool is_valid_nonroot(NodeIndex x) ALWAYS_INLINE { return !is_root_or_nil(x); }

private:
    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;

    #ifndef NDEBUG
    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        return nodes_[parent].children.contains(node);
    }
    #endif

    void relink_children(NodeIndex const node, NodeIndex const to) {
        auto& v = nodes_[node];
        for(size_t i = 0; i < v.children.size(); i++) {
            auto const child = v.children[i];
            nodes_[child].parent = to;
        }
    }

    void swap_independent(NodeIndex const a, NodeIndex const b) {
        auto const parent_a = nodes_[a].parent;
        auto const parent_b = nodes_[b].parent;

        assert(parent_a != parent_b);
        //assert(parent_a != b);
        //assert(parent_b != a);

        // re-link parent pointers of A's children to B
        relink_children(a, b);

        // replace A by B as child of parent of A
        if(!allow_orphans_ || is_valid(parent_a))
        {
            assert(is_child_of(a, parent_a));
            nodes_[parent_a].children.replace(a, b);
            assert(is_child_of(b, parent_a));
            assert(!is_child_of(a, parent_a));
        }

        // re-link parent pointers of B's children to A
        relink_children(b, a);

        // replace B by A as child of parent of B
        if(!allow_orphans_ || is_valid(parent_b))
        {
            assert(is_child_of(b, parent_b));
            nodes_[parent_b].children.replace(b, a);
            assert(is_child_of(a, parent_b));
            assert(!is_child_of(b, parent_b));
        }

        // swap in node array
        std::swap(nodes_[a], nodes_[b]);
    }

    void swap_siblings(NodeIndex const a, NodeIndex const b) {
        auto const parent = nodes_[a].parent;
        assert(nodes_[b].parent == parent);

        // re-link parent pointers of A's children to B
        relink_children(a, b);

        // re-link parent pointers of B's children to A
        relink_children(b, a);

        // swap A and B in the parent's list of children
        if(!allow_orphans_ || is_valid(parent)) {
            assert(is_child_of(a, parent));
            assert(is_child_of(b, parent));
            nodes_[parent].children.swap(a, b);
            assert(is_child_of(a, parent));
            assert(is_child_of(b, parent));
        }

        // swap in node array
        std::swap(nodes_[a], nodes_[b]);
    }

    void swap_parent_child(NodeIndex const a, NodeIndex const b) {
        swap_independent(a, b);
        /*
        auto const parent_a = nodes_[a].parent;
        assert(nodes_[b].parent == a);
        assert(is_child_of(b, a));

        // re-link parent pointers of A's children to B
        // nb: this induces a cycle where B's parent is B itself, but the final swap will take care of this
        // nb: it is VERY IMPORTANT that this happens first!
        relink_children(a, b);
        assert(nodes_[b].parent == b); // nb: see note above

        // replace A by B as child of parent of A
        if(!allow_orphans_ || is_valid(parent_a))
        {
            assert(is_child_of(a, parent_a));
            nodes_[parent_a].children.replace(a, b);
            assert(is_child_of(b, parent_a));
            assert(!is_child_of(a, parent_a));
        }

        // re-link parent pointers of B's children to A
        relink_children(b, a);

        // replace B by A as child of A
        // nb: again, we introduce a temporary cycle
        {
            assert(is_child_of(b, a));
            nodes_[a].children.replace(b, a);
            assert(is_child_of(a, a)); // nb: see note above
            assert(!is_child_of(a, parent_a));
        }

        // swap in node array
        std::swap(nodes_[a], nodes_[b]);
        */
    }

public:
    Trie(NodeIndex const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(NodeIndex i = 0; i < capacity_; i++) {
            nodes_[i] = Node(NIL, {});
        }
    }

    void fill() {
        size_ = capacity_;
    }

    Node& insert_child(NodeIndex const node, NodeIndex const parent, Character const label) {
        assert(is_valid(parent));
        assert(node < capacity_);
        assert(node != parent);
        assert(!is_child_of(node, parent));

        NodeIndex discard;
        assert(!try_get_child(parent, label, discard));

        nodes_[parent].children.insert(label, node);
        nodes_[node] = Node(parent, label);

        assert(is_leaf(node));
        
        #ifndef NDEBUG
        verify(parent);
        verify(node);
        #endif

        return nodes_[node];
    }

    // extract node from trie and return parent
    NodeIndex extract(NodeIndex const node) {
        if constexpr(!allow_orphans_) assert(is_leaf(node)); // cannot extract an inner node

        auto& v = nodes_[node];

        // orphan node
        auto const parent = v.parent;
        if constexpr(!allow_orphans_) assert(is_valid(parent));
        if(!allow_orphans_ || is_valid(parent)) {
            auto const label = v.inlabel;
            assert(is_child_of(node, parent));
            nodes_[parent].children.remove(label);

            NodeIndex discard;
            assert(!try_get_child(parent, label, discard));
            assert(!is_child_of(node, parent));
        }
        v.parent = NIL;

        if constexpr(allow_orphans_) {
            // orphan all children
            for(size_t i = 0; i < v.children.size(); i++) {
                nodes_[v.children[i]].parent = NIL;
            }

            // clear children
            v.children.clear();
        }

        #ifndef NDEBUG
        if(is_valid(parent)) verify(parent);
        verify(node);
        #endif

        return parent;
    }

    void swap(NodeIndex const a, NodeIndex const b) {
        assert(a != b);
        assert(!is_root(a));
        assert(!is_root(b));

        #ifndef NDEBUG
        verify(a);
        verify(b);
        #endif

        auto const parent_a = nodes_[a].parent;
        auto const parent_b = nodes_[b].parent;

        enum Case {
            independent,
            siblings,
            a_parent_of_b,
            b_parent_of_a,
        };
        Case whazzup; // nb: for debugging

        if(parent_b == a) {
            whazzup = Case::a_parent_of_b;
            swap_parent_child(a, b);
        } else if(parent_a == b) {
            whazzup = Case::b_parent_of_a;
            swap_parent_child(b, a);
        } else if(parent_a == parent_b) {
            whazzup = Case::siblings;
            swap_siblings(a, b);
        } else {
            whazzup = Case::independent;
            swap_independent(a, b);
        }

        // verify integrity of things
        #ifndef NDEBUG
        verify(a);
        verify(b);
        #endif
    }

    #ifndef NDEBUG
    void verify(NodeIndex const node) {
        auto& v = nodes_[node];

        // verify relationship to parent
        auto const parent = v.parent;
        assert(parent != node);

        if(is_valid(parent)) {
            assert(is_child_of(node, parent));

            NodeIndex found_in_parent;
            assert(try_get_child(parent, v.inlabel, found_in_parent));
            assert(found_in_parent == node);
        }

        // verify relationship to children
        for(size_t i = 0; i < v.children.size(); i++) {
            auto const child = v.children[i];

            assert(child != node);
            assert(nodes_[child].parent == node);
            
            NodeIndex found_child;
            assert(try_get_child(node, nodes_[child].inlabel, found_child));
            assert(found_child == child);
        }
    }
    #endif

    NodeIndex new_node() ALWAYS_INLINE {
        assert(size_ < capacity_);
        return size_++;
    }

    bool try_get_child(NodeIndex const node, Character const label, NodeIndex& out_child) const ALWAYS_INLINE {
        return nodes_[node].children.try_get(label, out_child);
    }

    NodeIndex child_count(NodeIndex const node) const {
        return nodes_[node].children.size();
    }

    NodeIndex index_in_parent(NodeIndex const node) const {
        auto const parent = nodes_[node].parent;
        auto const label = nodes_[node].inlabel;
        return nodes_[parent].children.find(label);
    }

    NodeIndex depth(NodeIndex const node) const {
        NodeIndex d = 0;
        auto v = node;
        while(is_valid_nonroot(v)) {
            ++d;
            v = parent(v);
        }
        return d;
    }

    bool is_leaf(NodeIndex const node) const {
        return nodes_[node].is_leaf();
    }

    NodeIndex root() const {
        return ROOT;
    }

    NodeIndex parent(NodeIndex const node) const {
        return nodes_[node].parent;
    }

    bool full() const { 
        return size_ == capacity_;
    }

    NodeIndex size() const {
        return size_;
    }

    Node& node(NodeIndex const v) {
        return nodes_[v];
    }

    Node const& node(NodeIndex const v) const {
        return nodes_[v];
    }

    size_t spell_reverse(NodeIndex const node, Character* buffer) const {
        size_t d = 0;
        auto v = node;
        while(is_valid_nonroot(v)) {
            buffer[d++] = nodes_[v].inlabel;
            v = nodes_[v].parent;
        }
        return d;
    }

    size_t spell(NodeIndex const node, Character* buffer) const {
        auto const d = spell_reverse(node, buffer);
        std::reverse(buffer, buffer +  d);
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
