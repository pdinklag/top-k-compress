#include <algorithm>
#include <bit>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <string>

#include <ankerl/unordered_dense.h>

#include "always_inline.hpp"

template<typename NodeData, typename EdgeHash, std::unsigned_integral NodeIndex = uint32_t>
class Trie {
private:
    using Character = char;
    using NodeSize = uint16_t; // we may need to store the value 256 itself
    using BitPack = uintmax_t;

    using UCharacter = std::make_unsigned_t<Character>;
    static constexpr uint64_t uchar_bits_ = std::numeric_limits<UCharacter>::digits;

    struct Node {
        NodeSize size;
        Character inlabel;
        NodeIndex parent;
        NodeData data;
        
        Node(NodeIndex const _parent, Character const _inlabel) : size(0), parent(_parent), inlabel(_inlabel) {
        }
        
        Node() : Node(0, 0) {
        }

        bool is_leaf() const ALWAYS_INLINE {
            return size == 0;
        }
    } __attribute__((packed));

    static constexpr uint64_t make_edge(NodeIndex const node, Character const c) {
        return ((uint64_t)node << uchar_bits_) | (UCharacter)c;
    }

    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;
    ankerl::unordered_dense::map<uint64_t, NodeIndex> edges_;

    #ifndef NDEBUG
    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        auto const e = make_edge(parent, nodes_[node].inlabel);
        return edges_.contains(e) && edges_.find(e)->second == node;
    }
    #endif

public:
    Trie(NodeIndex const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(NodeIndex i = 0; i < capacity_; i++) {
            nodes_[i] = Node();
        }
        edges_.reserve(capacity);
    }

    void insert_child(NodeIndex const node, NodeIndex const parent, Character const label) {
        assert(node < capacity_);

        NodeIndex discard;
        assert(!try_get_child(parent, label, discard));

        // possibly allocate child slots
        // nb: because we are only keeping track of the size and assume the capacity to always be its hyperceil,
        //     we may re-allocate here even though it would not be necessary
        //     however, if there were many removals, this may also actually shrink the capacity (unknowingly)
        //     in any event: it's not a bug, it's a feature!
        auto& p = nodes_[parent];
        
        // insert child
        ++p.size;

        auto const e = make_edge(parent, label);
        assert(!edges_.contains(e));
        edges_.emplace(e, node);
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

        auto const e = make_edge(parent, label);
        if(edges_.erase(e)) {
            --p.size;
        } else {
            assert(false); // "this kid is not my son"
        }

        assert(!is_child_of(node, parent));
        return parent;
    }

    NodeIndex new_node() {
        return size_++;
    }

    bool try_get_child(NodeIndex const node, Character const label, NodeIndex& out_child) ALWAYS_INLINE {
        auto const e = make_edge(node, label);
        auto const it = edges_.find(e);
        if(it != edges_.end()) {
            assert(nodes_[it->second].parent == node);
            out_child = it->second;
            return true;
        } else {
            return false;
        }
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
        size_t num_le4 = 0;
        size_t num_leaves = 0;
        
        for(size_t i = 0; i < capacity_; i++) {
            auto& v = nodes_[i];

            if(v.size == 0) ++num_leaves;
            if(v.size <= 4) ++num_le4;
        }

        std::cout << "trie info"
                  << ": sizeof(Node)=" << sizeof(Node)
                  << ", sizeof(NodeData)=" << sizeof(NodeData)
                  << ", num_leaves=" << num_leaves
                  << ", num_le4=" << num_le4
                  << std::endl;
    }
};
