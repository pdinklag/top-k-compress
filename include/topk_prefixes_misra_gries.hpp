#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <functional>
#include <memory>
#include <random>
#include <vector>
#include <unordered_map>

#include "always_inline.hpp"
#include "trie.hpp"
#include "trie_node.hpp"
#include "display.hpp"
#include "space_saving.hpp"

template<std::unsigned_integral TrieNodeIndex = uint32_t>
class TopKPrefixesMisraGries {
private:
    static constexpr bool gather_stats_ = true;

    struct NodeData;
    static constexpr auto NIL = SpaceSaving<NodeData>::NIL;

    struct NodeData : public TrieNode<TrieNodeIndex> {
        using Character = TrieNode<TrieNodeIndex>::Character;
        using Index = TrieNode<TrieNodeIndex>::Index;

    private:
        TrieNodeIndex freq_; // the current frequency
        TrieNodeIndex prev_; // the previous node in frequency order
        TrieNodeIndex next_; // the next node in frequency order

    public:
        NodeData() {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c), freq_(0), prev_(NIL), next_(NIL) {
        }

        // SpaceSavingItem
        TrieNodeIndex freq() const ALWAYS_INLINE { return freq_; }
        TrieNodeIndex prev() const ALWAYS_INLINE { return prev_; }
        TrieNodeIndex next() const ALWAYS_INLINE { return next_; }
        
        bool is_linked() const ALWAYS_INLINE { return this->is_leaf(); }

        void freq(TrieNodeIndex const f) ALWAYS_INLINE { freq_ = f; }
        void prev(TrieNodeIndex const x) ALWAYS_INLINE { prev_ = x; }
        void next(TrieNodeIndex const x) ALWAYS_INLINE { next_ = x; }
    } __attribute__((packed));

    using TrieNodeDepth = TrieNodeIndex;

    size_t k_;

    Trie<NodeData> trie_;
    SpaceSaving<NodeData> space_saving_;

    bool insert(TrieNodeIndex const parent, char const label, TrieNodeIndex& out_node) ALWAYS_INLINE {
        TrieNodeIndex v;
        if(space_saving_.get_garbage(v)) {
            // recycle something from the garbage
            assert(v < k_);
            assert(v != 0);

            auto& vdata = trie_.node(v);
            assert(vdata.is_leaf());
            assert(vdata.freq() <= space_saving_.threshold());

            // extract from trie
            auto const old_parent = trie_.extract(v);

            // old parent may have become a leaf
            if(trie_.is_valid_nonroot(old_parent) && trie_.is_leaf(old_parent)) {
                space_saving_.link(old_parent);
            }

            // new parent can no longer be a leaf
            if(trie_.is_valid_nonroot(parent) && trie_.is_leaf(parent)) {
                space_saving_.unlink(parent);
            }

            // insert into trie with new parent
            trie_.insert_child(v, parent, label);

            // now simply increment
            space_saving_.increment(v);

            out_node = v;
            return true;
        } else {
            // there is no garbage to recycle
            out_node = trie_.root();
            return false;
        }
    }

public:
    inline TopKPrefixesMisraGries() : k_(0) {
    }

    inline TopKPrefixesMisraGries(size_t const k, size_t const sketch_columns, size_t const fp_window_size = 8)
        : trie_(k),
          k_(k),
          space_saving_(trie_.nodes(), 1, k_ - 1, sketch_columns - 1) {
        
        // initialize all k nodes as orphans in trie
        trie_.fill();

        // make all of them garbage (except the root)
        space_saving_.init_garbage();
    }

    TopKPrefixesMisraGries(TopKPrefixesMisraGries&&) = default;
    TopKPrefixesMisraGries& operator=(TopKPrefixesMisraGries&&) = default;

    TopKPrefixesMisraGries(TopKPrefixesMisraGries const& other) {
        *this = other;
    }

    TopKPrefixesMisraGries& operator=(TopKPrefixesMisraGries const& other) {
        k_ = other.k_;
        trie_ = other.trie_;
        space_saving_ = other.space_saving_;
        space_saving_.set_items(trie_.nodes());
        return *this;
    }

    struct StringState {
        TrieNodeIndex len;         // length of the string
        TrieNodeIndex node;        // the string's node in the trie filter
        bool          frequent;    // whether or not the string is frequent
    };

    // returns a string state for the empty string to start with
    StringState empty_string() const ALWAYS_INLINE {
        StringState s;
        s.len = 0;
        s.node = trie_.root();
        s.frequent = true;
        return s;
    }

    // extends a string to the right by a new character
    StringState extend(StringState const& s, char const c) ALWAYS_INLINE {
        auto const i = s.len;

        // try and find extension in trie
        StringState ext;
        ext.len = i + 1;

        bool const edge_exists = s.frequent && trie_.try_get_child(s.node, c, ext.node);
        if(edge_exists) {
            // the current prefix is frequent, increment
            space_saving_.increment(ext.node);

            // done
            ext.frequent = true;
        } else {
            // the current prefix is non-frequent

            // attempt to insert it
            if(!insert(s.node, c, ext.node)) {
                // that failed, decrement everything else in turn
                space_saving_.decrement_all();
            }

            // we dropped out of the trie, so no extension can be frequent (not even if the current prefix was inserted or swapped in)
            ext.frequent = false;
        }

        // advance
        return ext;
    }

    // read the string with the given index into the buffer
    TrieNodeDepth get(TrieNodeIndex const index, char* buffer) const {
        return trie_.spell(index, buffer);
    }

    // try to find the string in the trie and report its depth and node
    TrieNodeDepth find(char const* s, size_t const max_len, TrieNodeIndex& out_node) const {
        auto v = trie_.root();
        TrieNodeDepth dv = 0;
        while(dv < max_len) {
            TrieNodeIndex u;
            if(trie_.try_get_child(v, s[dv], u)) {
                v = u;
                ++dv;
            } else {
                break;
            }
        }

        out_node = v;
        return dv;
    }

private:
    template<typename M>
    void get_string_freq_mapping(TrieNodeIndex const v, std::string const& prefix, M& out_map) {
        auto const& node = trie_.node(v);
        auto s = prefix + node.inlabel;
        out_map.emplace(s, trie_.freq());

        for(size_t i = 0; i < node.children.size(); i++) {
            get_string_freq_mapping(node.children[i], s, out_map);
        }
    }

public:
    auto get_string_freq_mapping() const {
        std::unordered_map<std::string, TrieNodeIndex> map;
        map.reserve(k_);

        // get mapping recursively for children of root
        std::string mt;
        auto const& root = trie_.node(trie_.root());
        for(size_t i = 0; i < root.children.size(); i++) {
            get_string_freq_mapping(root.children[i], mt, map);
        }

        return map;
    }

    void print_snapshot() const {
        trie_.print_snapshot();
        space_saving_.print_snapshot();        
    }

    void print_debug_info() const {
        trie_.print_debug_info();
        space_saving_.print_debug_info();
    }

    using TrieType = Trie<NodeData>;

    TrieType&& trie() {
        return std::move(trie_);
    }
};
