#pragma once

#include <bit>
#include <memory>

#include <word_packing.hpp>

template<bool with_parents_ = false>
class SmallTrie {
private:
    using Pack = uintmax_t;

    static constexpr size_t sigma_ = 256;
    static constexpr size_t bits_per_label_ = std::bit_width(sigma_ - 1);
    static constexpr size_t bits_per_size_ = std::bit_width(sigma_);

    size_t size_;
    size_t bits_per_ptr_;
    std::unique_ptr<Pack[]> node_sizes_;
    std::unique_ptr<Pack[]> node_children_;
    std::unique_ptr<Pack[]> child_labels_;
    std::unique_ptr<Pack[]> child_nodes_;
    std::unique_ptr<Pack[]> parents_;

    template<typename Trie>
    size_t construct(Trie const& other, size_t const other_v, size_t const parent, size_t& num_nodes, size_t& num_edges) {
        auto node_sizes = word_packing::accessor(node_sizes_.get(), bits_per_size_);
        
        auto const& children = other.children_of(other_v);
        auto const num_children = children.size();
        
        auto const v = num_nodes++;
        node_sizes[v] = num_children;

        if constexpr(with_parents_) {
            auto parents = word_packing::accessor(parents_.get(), bits_per_ptr_);
            parents[v] = parent;
        }
        
        if(children.size() > 0) {
            auto node_children = word_packing::accessor(node_children_.get(), bits_per_ptr_);
            auto child_labels = word_packing::accessor(child_labels_.get(), bits_per_label_);
            auto child_nodes = word_packing::accessor(child_nodes_.get(), bits_per_ptr_);

            auto const first_edge = num_edges;
            node_children[v] = first_edge;
            num_edges += num_children;

            for(size_t i = 0; i < num_children; i++) {
                child_labels[first_edge + i] = children.label(i);
                child_nodes[first_edge + i] = construct(other, children[i], v, num_nodes, num_edges);
            }
        }

        return v;
    }

    struct ChildrenProxy {
        SmallTrie const* trie;
        size_t v;

        size_t size() const {
            auto node_sizes = word_packing::accessor(trie->node_sizes_.get(), bits_per_size_);
            return node_sizes[v];
        }

        size_t operator[](size_t const i) const {
            auto node_children = word_packing::accessor(trie->node_children_.get(), trie->bits_per_ptr_);
            auto child_nodes = word_packing::accessor(trie->child_nodes_.get(), trie->bits_per_ptr_);
            return child_nodes[node_children[v] + i];
        }

        char label(size_t const i) const {
            auto node_children = word_packing::accessor(trie->node_children_.get(), trie->bits_per_ptr_);
            auto child_labels = word_packing::accessor(trie->child_labels_.get(), bits_per_label_);
            return (char)child_labels[node_children[v] + i];
        }
    };

public:
    SmallTrie() : size_(0) {
    }

    SmallTrie(SmallTrie&&) = default;
    SmallTrie& operator=(SmallTrie&&) = default;

    SmallTrie(SmallTrie const&) = delete;
    SmallTrie& operator=(SmallTrie const&) = delete;

    template<typename Trie>
    SmallTrie(Trie const& other) : size_(other.size()) {
        bits_per_ptr_ = std::bit_width(size_ - 1);

        node_sizes_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(size_, bits_per_size_));
        node_children_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(size_, bits_per_ptr_));
        child_labels_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(size_, bits_per_label_));
        child_nodes_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(size_, bits_per_ptr_));

        if constexpr(with_parents_) {
            parents_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(size_, bits_per_ptr_));
        }

        size_t num_nodes = 0;
        size_t num_edges = 0;
        construct(other, other.root(), 0, num_nodes, num_edges);
    }

    size_t root() const { return 0; }
    size_t size() const { return size_; }

    auto children_of(size_t const v) const {
        return ChildrenProxy { this, v };
    }

    template<std::unsigned_integral NodeIndex>
    bool try_get_child(size_t const v, char const c, NodeIndex& out_node) const {
        auto node_sizes = word_packing::accessor(node_sizes_.get(), bits_per_size_);
        auto const num_children = node_sizes[v];
        if(num_children > 0) {
            auto node_children = word_packing::accessor(node_children_.get(), bits_per_ptr_);
            auto const first_edge = node_children[v];

            auto child_labels = word_packing::accessor(child_labels_.get(), bits_per_label_);
            for(size_t i = 0; i < num_children; i++) {
                if(c == (char)child_labels[first_edge + i]) {
                    auto child_nodes = word_packing::accessor(child_nodes_.get(), bits_per_ptr_);
                    out_node = child_nodes[first_edge + i];
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    size_t spell_reverse(size_t const node, char* buffer) const {
        if constexpr(with_parents_) {
            auto parents = word_packing::accessor(parents_.get(), bits_per_ptr_);
            auto node_children = word_packing::accessor(node_children_.get(), bits_per_ptr_);
            auto child_labels = word_packing::accessor(child_labels_.get(), bits_per_label_);
            auto child_nodes = word_packing::accessor(child_nodes_.get(), bits_per_ptr_);

            size_t d = 0;
            auto v = node;
            while(v) {
                auto const parent = parents[v];

                // find node in parent
                size_t i = node_children[parent];
                while(child_nodes[i] != v) ++i;

                // append label to buffer
                *buffer++ = child_labels[i];

                // navigate up
                ++d;
                v = parent;
            }
            return d;
        } else {
            return 0;
        }
    }

    size_t spell(size_t const node, char* buffer) const {
        auto const d = spell_reverse(node, buffer);
        std::reverse(buffer, buffer +  d);
        return d;
    }

    void print_debug_info() const {
    }

    size_t mem_size() const {
        return sizeof(SmallTrie) +
            word_packing::num_packs_required<Pack>(size_, bits_per_size_) +
            word_packing::num_packs_required<Pack>(size_, bits_per_label_) +
            (with_parents_ ? 3 : 2) * word_packing::num_packs_required<Pack>(size_, bits_per_ptr_);
    }
};
