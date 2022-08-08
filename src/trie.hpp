#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

template<typename NodeData>
class Trie {
private:
    using NodeSize = uint16_t; // we may need to store the value 256 itself

    struct Node {
        NodeSize size;
        NodeSize capacity;
        char* labels;
        size_t* children;
        
        size_t parent;
        char label;

        NodeData data;
        
        Node(size_t const _parent, char const _label)
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

    size_t capacity_;
    size_t size_;

    std::unique_ptr<Node[]> nodes_;

    bool is_child_of(size_t const node, size_t const parent) const {
        for(size_t i = 0; i < nodes_[parent].size; i++) {
            if(nodes_[parent].children[i] == node) {
                return true;
            }
        }
        return false;
    }

public:
    Trie(size_t const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(size_t i = 0; i < capacity_; i++) {
            nodes_[i] = Node();
        }
    }

    void insert_child(size_t const node, size_t const parent, char const label) {
        size_t discard;
        assert(!try_get_child<false>(parent, label, discard));

        //auto sibling = nodes_[parent].first_child;
        //nodes_[parent].first_child = node;
        
        // possibly allocate new child slots
        auto& p = nodes_[parent];
        if(p.size >= p.capacity) {
            size_t const new_cap = std::max(size_t(1), 2 * (size_t)p.capacity);
            auto* new_labels = new char[new_cap];
            auto* new_children = new size_t[new_cap];
            
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
    size_t extract(size_t const node) {
        assert(is_leaf(node)); // cannot extract an inner node

        auto const parent = nodes_[node].parent;
        assert(is_child_of(node, parent));
        
        auto const label = nodes_[node].label;
        
        auto& p = nodes_[parent];
        size_t i = 0;
        while(i < p.size && p.labels[i] != label) ++i; // nb: we could also search in the child array, but searching in the labels causes fewer cache misses
        
        if(i < p.size) {
            assert(p.children[i] == node);
            
            // swap with last child if necessary and reduce size
            if(p.size > 1) {
                size_t const last = p.size - 1;
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

    size_t new_node() {
        return size_++;
    }

    template<bool mtf = true>
    bool try_get_child(size_t const node, char const label, size_t& out_child) {
        auto const& v = nodes_[node];
        for(size_t i = 0; i < v.size; i++) {
            if(v.labels[i] == label) {
                out_child = v.children[i];
                if constexpr(mtf) {
                    // no longer implemented since switching to label-string-representation
                }
                return true;
            }
        }
        return false;
    }

    bool is_leaf(size_t const node) const {
        return nodes_[node].is_leaf();
    }

    size_t root() const {
        return 0;
    }

    bool full() const { 
        return size_ == capacity_;
    }

    NodeData& data(size_t const node) {
        return nodes_[node].data;
    }

    NodeData const& data(size_t const node) const {
        return nodes_[node].data;
    }

    size_t spell(size_t const node, char* buffer) const {
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
