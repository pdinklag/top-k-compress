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
#include "display.hpp"
#include "trie_concepts.hpp"

template<trie_node Node>
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

public:
    Trie() : capacity_(0), size_(0) {
    }

    Trie(NodeIndex const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(NodeIndex i = 0; i < capacity_; i++) {
            nodes_[i] = Node(NIL, {});
        }
    }

    Trie(Trie&&) = default;
    Trie& operator=(Trie&&) = default;

    Trie(Trie const& other) {
        *this = other;
    }

    Trie& operator=(Trie const& other) {
        capacity_ = other.capacity_;
        size_ = other.size_;
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(NodeIndex i = 0; i < capacity_; i++) {
            nodes_[i] = other.nodes_[i];
        }
        return *this;
    }

    void fill() {
        size_ = capacity_;
    }

    Node& insert_child(NodeIndex const node, NodeIndex const parent, Character const label) {
        assert(is_valid_nonroot(node));
        assert(is_valid(parent));
        assert(node < capacity_);
        assert(node != parent);
        assert(!is_child_of(node, parent));

        NodeIndex discard;
        assert(!try_get_child(parent, label, discard));

        nodes_[parent].children.insert(label, node);
        nodes_[node].parent = parent;
        nodes_[node].inlabel = label;
        nodes_[node].children.clear();

        assert(is_leaf(node));

        return nodes_[node];
    }

    // extract node from trie and return parent
    NodeIndex extract(NodeIndex const node) {
        assert(!is_root(node));
        assert(is_leaf(node)); // cannot extract an inner node

        auto& v = nodes_[node];

        // orphan node
        auto const parent = v.parent;
        if(is_valid(parent)) {
            auto const label = v.inlabel;
            assert(is_child_of(node, parent));
            nodes_[parent].children.remove(label);

            NodeIndex discard;
            assert(!try_get_child(parent, label, discard));
            assert(!is_child_of(node, parent));
        }
        v.parent = NIL;

        return parent;
    }

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

    Node* nodes() {
        return nodes_.get();
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

    void print_snapshot() const {
        print_debug_info();
    }

    void print_debug_info() const {
        size_t num_leaves = 0;
        size_t num_small = 0;
        
        for(size_t i = 0; i < size_; i++) {
            auto& v = nodes_[i];

            if(v.size() == 0) ++num_leaves;
            if(v.size() <= Node::ChildArray::inline_size_) ++num_small;
        }

        std::cout << "# DEBUG: trie"
                  << ", sizeof(Node)=" << sizeof(Node)
                  << ", small_node_size_=" << Node::ChildArray::inline_size_
                  << ", small_node_align_=" << Node::ChildArray::inline_align_
                  << ", num_nodes=" << size_
                  << ", num_leaves=" << num_leaves
                  << ", num_small=" << num_small
                  << std::endl;
    }

private:
    void dfo(NodeIndex* map, NodeIndex const v, NodeIndex& rank) const {
        map[v] = rank++;

        auto const& children = nodes_[v].children;
        for(size_t i = 0; i < children.size(); i++) {
            dfo(map, children[i], rank);
        }
    }

public:
    void dfo(NodeIndex* map) const {
        NodeIndex rank = 0;
        dfo(map, root(), rank);
    }

    void renumber(std::function<NodeIndex(NodeIndex)> map) {
        // renumber node links
        for(size_t i = 0; i < capacity_; i++) {
            nodes_[i].renumber(map);
        }

        // physically reorder
        auto new_nodes = std::make_unique<Node[]>(capacity_);
        for(size_t i = 0; i < capacity_; i++) {
            new_nodes[map(i)] = std::move(nodes_[i]);
        }
        nodes_ = std::move(new_nodes);
    }

    struct Analysis {
        size_t leaves;
        size_t arms_num;
        size_t arms_total;
        size_t arms_longest;
        size_t outd_total;
        size_t outd_max;
        size_t ioutd_total;
        size_t ioutd_max;

        Analysis()
            : leaves(0),
              arms_num(0),
              arms_total(0),
              arms_longest(0),
              outd_total(0),
              outd_max(0),
              ioutd_total(0) {
        }
    };

private:
    void to_bp(std::ostringstream& out, NodeIndex const v) const {
        out << "(";
        auto const& children = nodes_[v].children;
        for(size_t i = 0; i < children.size(); i++) {
            to_bp(out, children[i]);
        }
        out << ")";
    }

    void analyze_arms(Analysis& ana, NodeIndex const v, size_t const arm_len) const { 
        auto const& children = nodes_[v].children;
        if(children.size() == 0) {
            // we reached a leaf
            ++ana.arms_num;
            ana.arms_total += arm_len + 1;
            ana.arms_longest = std::max(ana.arms_longest, arm_len + 1);
        } else {
            for(size_t i = 0; i < children.size(); i++) {
                analyze_arms(ana, children[i], (children.size() == 1) ? arm_len + 1 : 0);
            }
        }
    }

    void analyze_outd(Analysis& ana) const {
        for(size_t v = 0; v < size(); v++) {
            size_t const outd = nodes_[v].children.size();
            ana.outd_total += outd;
            ana.outd_max = std::max(ana.outd_max, outd);
            if(outd > 0) {
                ana.ioutd_total += outd;
            } else {
                ++ana.leaves;
            }
        }
    }

public:
    std::string to_bp() const {
        std::ostringstream out;
        to_bp(out, root());
        return out.str();
    }

    Analysis analyze() const {
        Analysis ana;
        analyze_arms(ana, root(), 0);
        analyze_outd(ana);
        return ana;
    }
};
