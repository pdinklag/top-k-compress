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
    using UCharacter = uint8_t;
    using NodeSize = uint16_t; // we may need to store the value 256 itself
    using BitPack = uintmax_t;

    static constexpr size_t bits_per_pack_ = 8 * sizeof(BitPack);
    static constexpr size_t bit_pack_mask_ = bits_per_pack_ - 1;
    static constexpr size_t sigma_ = ((size_t)std::numeric_limits<UCharacter>::max() + 1);
    static_assert((sigma_ % bits_per_pack_) == 0);
    static constexpr size_t num_bit_packs_ = sigma_ / bits_per_pack_;

    struct NodeChildrenLarge {
        BitPack ind[num_bit_packs_];
        NodeIndex* children;

        #ifndef NDEBUG
        // only used for debugging
        size_t size() const ALWAYS_INLINE {
            size_t s = 0;
            for(size_t i = 0; i < num_bit_packs_; i++) {
                s += std::popcount(ind[i]);
            }
            return s;
        }
        #endif

        void clear() ALWAYS_INLINE {
            for(size_t i = 0; i < num_bit_packs_; i++) {
                ind[i] = 0;
            }
        }

        void set(UCharacter const i) ALWAYS_INLINE {
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            ind[b] |= (1ULL << j);
        }

        void unset(UCharacter const i) ALWAYS_INLINE {
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            ind[b] &= ~(1ULL << j);
        }

        bool get(UCharacter const i) const ALWAYS_INLINE {
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            return (ind[b] & (1ULL << j)) != 0;
        }
        
        size_t rank(UCharacter const i) const ALWAYS_INLINE {
            assert(get(i));

            size_t r = 0;
            size_t const b = i / bits_per_pack_;
            size_t const j = i % bits_per_pack_;
            for(size_t i = 0; i < b; i++) {
                r += std::popcount(ind[i]);
            }

            BitPack const mask = std::numeric_limits<BitPack>::max() >> (std::numeric_limits<BitPack>::digits - 1 - j);
            return r + std::popcount(ind[b] & mask) - 1;
        }
    } __attribute__((packed));

    static constexpr size_t small_node_size_ = sizeof(NodeChildrenLarge) / (sizeof(NodeIndex) + sizeof(Character));
    static constexpr size_t small_node_align_ = sizeof(NodeChildrenLarge) - small_node_size_ * (sizeof(NodeIndex) + sizeof(Character));

    struct NodeChildrenSmall {
        NodeIndex children[small_node_size_];
        Character labels[small_node_size_];
    } __attribute__((packed));

    struct Node {
        static constexpr size_t capacity_for(size_t const n) {
            return (n > 0) ? std::bit_ceil(n) : 0;
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
            c.large.clear();
            c.large.children = nullptr;
        }
        
        Node() : Node(0, 0) {
        }

        ~Node() {
            if(!is_small()) {
                delete[] c.large.children;
            }
        }

        bool is_small() const ALWAYS_INLINE {
            return size <= small_node_size_;
        }

        bool is_leaf() const ALWAYS_INLINE {
            return size == 0;
        }
    } __attribute__((packed));

    NodeIndex capacity_;
    NodeIndex size_;

    std::unique_ptr<Node[]> nodes_;

    #ifndef NDEBUG
    bool is_child_of(NodeIndex const node, NodeIndex const parent) const {
        auto& p = nodes_[parent];
        if(p.is_small()) {
            for(NodeIndex i = 0; i < p.size; i++) {
                if(p.c.small.children[i] == node) {
                    return true;
                }
            }
        } else {
            for(NodeIndex i = 0; i < p.size; i++) {
                if(p.c.large.children[i] == node) {
                    return true;
                }
            }
        }
        return false;
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

        // possibly allocate child slots
        // nb: because we are only keeping track of the size and assume the capacity to always be its hyperceil,
        //     we may re-allocate here even though it would not be necessary
        //     however, if there were many removals, this may also actually shrink the capacity (unknowingly)
        //     in any event: it's not a bug, it's a feature!
        auto& p = nodes_[parent];
        if(p.size == small_node_size_ || (!p.is_small() && p.size == Node::capacity_for(p.size))) {
            auto* new_children = new NodeIndex[Node::capacity_for(p.size + 1)];
            if(p.is_small()) {
                assert(p.size == small_node_size_);

                // becoming large
                Character old_labels[small_node_size_];

                // we need to make sure now that the children are transferred in the order of their labels
                std::pair<UCharacter, NodeIndex> old_child_info[small_node_size_];
                for(size_t j = 0; j < p.size; j++) {
                    auto const child_j = p.c.small.children[j];
                    old_child_info[j] = { p.c.small.labels[j], child_j };
                }
                std::sort(old_child_info, old_child_info + p.size, [](auto const& a, auto const& b){ return a.first < b.first; });

                // clear all bits first
                p.c.large.clear();

                // set bits that need to be set and write children
                for(size_t j = 0; j < p.size; j++) {
                    p.c.large.set(old_child_info[j].first);
                    new_children[j] = old_child_info[j].second;
                }
            } else {
                // staying large
                std::copy(p.c.large.children, p.c.large.children + p.size, new_children);
                delete[] p.c.large.children;
            }
            p.c.large.children = new_children;
        }
        
        // insert child
        ++p.size;
        if(p.is_small()) {
            auto const i = p.size - 1;
            p.c.small.labels[i] = label;
            p.c.small.children[i] = node;
        } else {
            p.c.large.set(label);
            assert(p.size == p.c.large.size());

            auto const i = p.c.large.rank(label);
            for(size_t j = p.size - 1; j > i; j--) {
                p.c.large.children[j] = p.c.large.children[j - 1];
            }
            p.c.large.children[i] = node;
        }
        
        assert(nodes_[node].is_leaf()); // even though this makes no sense semantically, let's make sure we are not leaking a children array
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

        NodeIndex found = 0;
        if(p.is_small()) {
            while(found < p.size && p.c.small.labels[found] != label) ++found;
        } else {
            found = p.c.large.get(label) ? p.c.large.rank(label) : p.size;
        }

        auto const i = found;
        if(i < p.size) {
            // remove from child array if necessary
            if(p.is_small()) {
                assert(p.c.small.children[i] == node);

                // swap with last child if necessary
                if(p.size > 1) {
                    NodeIndex const last = p.size - 1;
                    p.c.small.labels[i] = p.c.small.labels[last];
                    p.c.small.children[i] = p.c.small.children[last];
                }
            } else {
                assert(p.c.large.children[i] == node);

                // remove
                p.c.large.unset(label);
                assert(p.size - 1 == p.c.large.size());

                if(p.size > 1) {
                    for(size_t j = i; j + 1 < p.size; j++) {
                        p.c.large.children[j] = p.c.large.children[j+1];
                    }
                }
            }

            // possibly convert into small node
            auto const was_small = p.is_small();
            --p.size; // nb: must be done before moving data

            if(!was_small && p.is_small()) {
                assert(p.size == small_node_size_);
                Character new_labels[small_node_size_];
                NodeIndex new_children[small_node_size_];

                {
                    size_t j = 0;
                    for(size_t c = 0; c < 256; c++) {
                        if(p.c.large.get(c)) {
                            new_labels[j] = (Character)c;
                            new_children[j] = p.c.large.children[j];
                            ++j;
                        }
                    }
                    assert(j == small_node_size_);
                }

                delete[] p.c.large.children;

                for(size_t j = 0; j < p.size; j++) {
                    p.c.small.labels[j] = new_labels[j];
                    p.c.small.children[j] = new_children[j];
                }
            }

            assert(!is_child_of(node, parent));
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
        if(v.is_small()) {
            for(NodeIndex i = 0; i < v.size; i++) {
                if(v.c.small.labels[i] == label) {
                    out_child = v.c.small.children[i];
                    return true;
                }
            }
            return false;
        } else {
            if(v.c.large.get(label)) {
                out_child = v.c.large.children[v.c.large.rank(label)];
                return true;
            } else {
                return false;
            }
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
