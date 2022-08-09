#include <algorithm>
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
    using NodeSize = uint16_t; // we may need to store the value 256 itself

    struct Node {
        NodeSize size;
        NodeSize capacity;
        char* labels;
        NodeIndex* children;
        
        NodeIndex parent;
        char label;

        NodeData data;
        
        Node(NodeIndex const _parent, char const _label)
            : size(0),
              capacity(0),
              labels(nullptr),
              children(nullptr),
              parent(_parent),
              label(_label) {
        }
        
        Node() : Node(0, 0) {
        }

        ~Node() {
            if(labels) delete[] labels;
            if(children) delete[] children;
        }

        inline bool is_leaf() const {
            return size == 0;
        }
    } __attribute__((packed));

    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;

    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        for(NodeIndex i = 0; i < nodes_[parent].size; i++) {
            if(nodes_[parent].children[i] == node) {
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

    void insert_child(NodeIndex const node, NodeIndex const parent, char const label) {
        NodeIndex discard;
        assert(!try_get_child(parent, label, discard));

        //auto sibling = nodes_[parent].first_child;
        //nodes_[parent].first_child = node;
        
        // possibly allocate new child slots
        auto& p = nodes_[parent];
        if(p.size >= p.capacity) {
            NodeIndex const new_cap = std::max(NodeIndex(1), 2 * (NodeIndex)p.capacity);
            auto* new_labels = new char[new_cap];
            auto* new_children = new NodeIndex[new_cap];
            
            std::copy(p.labels, p.labels + p.size, new_labels);
            std::copy(p.children, p.children + p.size, new_children);
            
            p.capacity = new_cap;

            if(p.labels) delete[] p.labels;
            if(p.children) delete[] p.children;

            p.labels = new_labels;
            p.children = new_children;
        }
        
        // append child
        p.labels[p.size] = label;
        p.children[p.size] = node;
        ++p.size;
        
        nodes_[node] = Node(parent, label);

        assert(is_child_of(node, parent));
        assert(is_leaf(node));
    }

    // extract node from trie and return parent
    NodeIndex extract(NodeIndex const node) {
        assert(is_leaf(node)); // cannot extract an inner node

        auto const parent = nodes_[node].parent;
        assert(is_child_of(node, parent));
        
        auto const label = nodes_[node].label;
        
        auto& p = nodes_[parent];
        NodeIndex i = 0;
        while(i < p.size && p.labels[i] != label) ++i; // nb: we could also search in the child array, but searching in the labels causes fewer cache misses
        
        if(i < p.size) {
            assert(p.children[i] == node);
            
            // swap with last child if necessary and reduce size
            if(p.size > 1) {
                NodeIndex const last = p.size - 1;
                std::swap(p.labels[i], p.labels[last]);
                std::swap(p.children[i], p.children[last]);
            }
            --p.size;
            // TODO: possibly re-allocate?
        } else {
            // "this kid is not my son"
            assert(false);
        }

        assert(nodes_[node].freq >= nodes_[node].insert_freq);
        return parent;
    }

    NodeIndex new_node() {
        return size_++;
    }

    bool try_get_child(NodeIndex const node, char const label, NodeIndex& out_child) ALWAYS_INLINE {
        auto const& v = nodes_[node];
        for(NodeIndex i = 0; i < v.size; i++) {
            if(v.labels[i] == label) {
                out_child = v.children[i];
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

    size_t spell(NodeIndex const node, char* buffer) const {
        // spell reverse
        size_t d = 0;
        auto v = node;
        while(v) {
            buffer[d++] = nodes_[v].label;
            v = nodes_[v].parent;
        }

        // reverse and return length
        std::reverse(buffer, buffer + d);
        return d;
    }
};
