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
#include "truncated_trie.hpp"
#include "min_pq.hpp"
#include "display.hpp"

template<std::unsigned_integral TrieNodeIndex = uint32_t>
class TopKPrefixesMisraGries {
private:
    static constexpr bool gather_stats_ = true;

    struct NodeData : public TrieNode<TrieNodeIndex> {
        using Character = TrieNode<TrieNodeIndex>::Character;
        using Index = TrieNode<TrieNodeIndex>::Index;

        size_t freq;                    // the current estimated frequency
        MinPQ<size_t>::Location minpq;  // the entry in the minimum PQ, if any

        void dump_extra_info() const {
            std::cout << " [freq=" << freq << "]";
        }

        NodeData() {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c) {
        }
    } __attribute__((packed));

    static constexpr bool DEBUG = false;

    using TrieNodeDepth = TrieNodeIndex;

    size_t k_;

    Trie<NodeData> trie_;
    MinPQ<size_t, TrieNodeIndex> min_pq_;
    size_t threshold_;

    // stats
    struct Stats {
        size_t max_freq = 0;
        size_t num_increment = 0;
        size_t num_increment_leaf = 0;
        size_t num_recycle = 0;
        size_t num_decrement = 0;
    } stats_;

    TrieNodeIndex recycle_min(TrieNodeIndex const parent, char const label) ALWAYS_INLINE {
        // extract minimum -- it must be a leaf
        if constexpr(gather_stats_) ++stats_.num_recycle;

        auto const recycle = min_pq_.extract_min();
        assert(trie_.is_leaf(recycle));

        auto const old_parent = trie_.extract(recycle);
        if(old_parent) {
            // the old parent may now be a leaf
            auto& old_parent_data = trie_.node(old_parent);
            if(old_parent_data.is_leaf()) {
                // insert into min PQ
                old_parent_data.minpq = min_pq_.insert(old_parent, old_parent_data.freq);
            }
        }

        // insert the current string into the trie, reusing the old node ID
        trie_.insert_child(recycle, parent, label);
        auto& recycle_data = trie_.node(recycle);
        recycle_data.freq = threshold_ + 1;

        // also insert it into the min PQ since it is a leaf
        assert(trie_.is_leaf(recycle));
        recycle_data.minpq = min_pq_.insert(recycle, threshold_ + 1);
        
        // the parent is no longer a leaf, so remove from min PQ
        if(parent) {
            auto& parent_data = trie_.node(parent);
            parent_data.minpq = min_pq_.remove(parent_data.minpq);
        }

        return recycle;
    }

    void decrement_all() ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_decrement;
        ++threshold_;
    }

    void increment(TrieNodeIndex const v) ALWAYS_INLINE {
        // increment frequency
        if constexpr(gather_stats_) ++stats_.num_increment;

        auto& data = trie_.node(v);
        ++data.freq;
        if constexpr(gather_stats_) stats_.max_freq = std::max(data.freq, stats_.max_freq);

        // increment in min PQ
        if(trie_.is_leaf(v)) {
            if constexpr(gather_stats_) ++stats_.num_increment_leaf;
            assert((bool)data.minpq);
            data.minpq = min_pq_.increase_key(data.minpq);
        }
    }

    TrieNodeIndex insert(TrieNodeIndex const parent, char const label) ALWAYS_INLINE {
        auto const v = trie_.new_node();
        trie_.insert_child(v, parent, label);

        // remove the parent from the trie (it is no longer a leaf)
        auto& parent_data = trie_.node(parent);
        assert(!parent_data.is_leaf());
        parent_data.minpq = min_pq_.remove(parent_data.minpq);

        // insert into min PQ
        auto& data = trie_.node(v);
        assert(data.is_leaf());
        data.freq = threshold_ + 1;
        data.minpq = min_pq_.insert(v, threshold_ + 1);

        assert(min_pq_.freq(data.minpq) == threshold_ + 1);
        return v;
    }

public:
    inline TopKPrefixesMisraGries(size_t const k, size_t const sketch_columns, size_t const fp_window_size = 8)
        : trie_(k),
          min_pq_(k),
          k_(k),
          threshold_(0) {
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
            increment(ext.node);

            // done
            ext.frequent = true;
        } else {
            // the current prefix is non-frequent
            if(trie_.full()) {
                // the trie is full
                if(min_pq_.min_frequency() <= threshold_) {
                    // there are garbage prefixes, recycle one
                    ext.node = recycle_min(s.node, c);
                } else {
                    // the trie is full and there is no garbage, increment the threshold
                    decrement_all();
                    ext.node = 0;
                }
            } else {
                // the trie is not yet full, insert the prefix
                ext.node = insert(s.node, c);
            }

            // we dropped out of the trie, so no extension can be frequent (not even if the current prefix was inserted or swapped in)
            ext.frequent = false;
        }

        // advance
        if constexpr(DEBUG) {
            std::cout << "top-k: extend string of length " << s.len << " (node " << s.node << ") by " << display(c) << " -> node=" << ext.node << ", fp=" << ext.fingerprint << ", frequent=" << ext.frequent << std::endl;
        }

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
        min_pq_.print_debug_info();

        std::cout << "# DEBUG: misra-gries" << ", threshold=" << threshold_;
        if constexpr(gather_stats_) {
            std::cout
                << ", max_freq=" << stats_.max_freq
                << ", num_increment=" << stats_.num_increment
                << ", num_increment_leaf=" << stats_.num_increment_leaf
                << ", num_decrement=" << stats_.num_decrement
                << ", num_recycle=" << stats_.num_recycle
                ;
        } else {
            std::cout << " (advanced stats disabled)";
        }

        std::cout << std::endl;
    }
};
