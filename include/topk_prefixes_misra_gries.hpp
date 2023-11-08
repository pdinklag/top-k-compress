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

#include <ankerl/unordered_dense.h>
#include <pm/stopwatch.hpp>
#include <tdc/util/concepts.hpp>

#include "always_inline.hpp"
#include "trie.hpp"
#include "trie_node.hpp"
#include "display.hpp"
#include "space_saving.hpp"

template<std::unsigned_integral TrieNodeIndex = uint32_t>
class TopKPrefixesMisraGries {
private:
    static constexpr bool gather_stats_ = true;

    static constexpr TrieNodeIndex NIL = -1;

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

    Trie<NodeData, true> trie_;
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
            space_saving_.unlink(parent);

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
    inline TopKPrefixesMisraGries(size_t const k, size_t const sketch_columns, size_t const fp_window_size = 8)
        : trie_(k),
          k_(k),
          space_saving_(trie_.nodes(), k_, sketch_columns - 1) {
        
        // initialize all k nodes as orphans in trie
        trie_.fill();

        // make all of them garbage (except the root)
        space_saving_.init_as_garbage(1, k_ - 1);
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

    void print_debug_info() const {
        trie_.print_debug_info();
        std::cout << "# DEBUG: misra-gries2" << ", threshold=" << space_saving_.threshold() << std::endl;
    }
};
