#include <algorithm>
#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "always_inline.hpp"

template<typename NodeData, std::unsigned_integral NodeIndex = uint32_t>
class Trie {
private:
    using Character = char;
    using NodeSize = uint16_t; // we may need to store the value 256 itself

    struct NodeChildrenLarge {
        Character* labels;
        NodeIndex* children;
    };

    static constexpr size_t small_node_size_ = sizeof(NodeChildrenLarge) / (sizeof(NodeIndex) + sizeof(Character));
    static constexpr size_t small_node_align_ = sizeof(NodeChildrenLarge) - small_node_size_ * (sizeof(NodeIndex) + sizeof(Character));

    struct NodeChildrenSmall {
        Character labels[small_node_size_];
        NodeIndex children[small_node_size_];
    };

    struct Node {
        static constexpr size_t capacity_for(size_t const n) {
            return (n == 0) ? 0 : std::bit_ceil(n);
        }

        NodeSize size;
        Character inlabel;
        NodeIndex parent;

        union {
            NodeChildrenLarge large;
            NodeChildrenSmall small;
        } c;
        
        NodeData data;
        
        Node(NodeIndex const _parent, Character const _inlabel) : size(0), parent(_parent), inlabel(_inlabel) {
            c.large.labels = nullptr;
            c.large.children = nullptr;
        }
        
        Node() : Node(0, 0) {
        }

        ~Node() {
            if(!is_small()) {
                delete[] c.large.labels;
                delete[] c.large.children;
            }
        }

        bool is_small() const ALWAYS_INLINE {
            return size <= small_node_size_;
        }

        bool is_leaf() const ALWAYS_INLINE {
            return size == 0;
        }
    };

    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;

    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        auto& p = nodes_[parent];
        auto const* const pchildren = p.is_small() ? p.c.small.children : p.c.large.children;
        for(NodeIndex i = 0; i < p.size; i++) {
            if(pchildren[i] == node) {
                return true;
            }
        }
        return false;
    }

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

        // possibly allocate child slots
        auto& p = nodes_[parent];

        auto const i = p.size;
        if(i == small_node_size_ || (!p.is_small() && i >= Node::capacity_for(p.size))) {
            auto const new_cap = Node::capacity_for(p.size + 1);
            auto* new_labels = new Character[new_cap];
            auto* new_children = new NodeIndex[new_cap];

            auto const* const old_labels = p.is_small() ? p.c.small.labels : p.c.large.labels;
            auto const* const old_children = p.is_small() ? p.c.small.children : p.c.large.children;

            for(size_t j = 0; j < p.size; j++) {
                new_labels[j] = old_labels[j];
                new_children[j] = old_children[j];
            }
            
            if(!p.is_small()) {
                delete[] p.c.large.labels;
                delete[] p.c.large.children;
            }

            p.c.large.labels = new_labels;
            p.c.large.children = new_children;
        }
        
        // append child
        ++p.size;
        if(p.is_small()) {
            p.c.small.labels[i] = label;
            p.c.small.children[i] = node;
        } else {
            p.c.large.labels[i] = label;
            p.c.large.children[i] = node;
        }
        
        nodes_[node] = Node(parent, label);

        assert(is_child_of(node, parent));
        assert(is_leaf(node));
    }

    // extract node from trie and return parent
    NodeIndex extract(NodeIndex const node) {
        assert(is_leaf(node)); // cannot extract an inner node

        auto const parent = nodes_[node].parent;
        assert(is_child_of(node, parent));
        
        auto const label = nodes_[node].inlabel;
        
        auto& p = nodes_[parent];
        NodeIndex i = 0;

        auto* plabels = p.is_small() ? p.c.small.labels : p.c.large.labels;
        while(i < p.size && plabels[i] != label) ++i; // nb: we could also search in the child array, but searching in the labels causes fewer cache misses
        
        if(i < p.size) {
            auto* pchildren = p.is_small() ? p.c.small.children : p.c.large.children;
            assert(pchildren[i] == node);
            
            // swap with last child if necessary and reduce size
            if(p.size > 1) {
                NodeIndex const last = p.size - 1;
                std::swap(plabels[i], plabels[last]);
                std::swap(pchildren[i], pchildren[last]);
            }

            // possibly convert into small node
            auto const was_small = p.is_small();
            --p.size; // nb: must be done before moving data
            if(!was_small && p.is_small()) {
                for(size_t j = 0; j < p.size; j++) {
                    p.c.small.labels[j] = plabels[j];
                    p.c.small.children[j] = pchildren[j];
                }

                delete[] plabels;
                delete[] pchildren;
            }
        } else {
            // "this kid is not my son"
            assert(false);
        }
        return parent;
    }

    NodeIndex new_node() {
        return size_++;
    }

    bool try_get_child(NodeIndex const node, Character const label, NodeIndex& out_child) ALWAYS_INLINE {
        auto const& v = nodes_[node];
        auto const* const vlabels = v.is_small() ? v.c.small.labels : v.c.large.labels;
        for(NodeIndex i = 0; i < v.size; i++) {
            if(vlabels[i] == label) {
                out_child = v.is_small() ? v.c.small.children[i] : v.c.large.children[i];
                return true;
            }
        }
        return false;
    }

    bool is_leaf(NodeIndex const node) const {
        return nodes_[node].is_leaf();
    }

    NodeIndex root() const {
        return 0;
    }

    bool full() const { 
        return size_ == capacity_;
    }

    NodeData& data(NodeIndex const node) {
        return nodes_[node].data;
    }

    NodeData const& data(NodeIndex const node) const {
        return nodes_[node].data;
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

            if(v.size == 0) ++num_leaves;
            if(v.size <= small_node_size_) ++num_small;
        }

        std::cout << "trie info"
                  << ": sizeof(Node)=" << sizeof(Node)
                  << ", sizeof(NodeData)=" << sizeof(NodeData)
                  << ", small_node_size_=" << small_node_size_
                  << ", small_node_align_=" << small_node_align_
                  << ", num_leaves=" << num_leaves
                  << ", num_small=" << num_small
                  << std::endl;
    }
};
