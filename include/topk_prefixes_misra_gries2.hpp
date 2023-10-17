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

template<std::unsigned_integral TrieNodeIndex = uint32_t>
class TopKPrefixesMisraGries2 {
private:
    struct NodeData : public TrieNode<TrieNodeIndex> {
        using Character = TrieNode<TrieNodeIndex>::Character;
        using Index = TrieNode<TrieNodeIndex>::Index;

        size_t freq; // the current estimated frequency

        void dump_extra_info() const {
            std::cout << " [freq=" << freq << "]";
        }

        NodeData() : freq(0) {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c), freq(0) {
        }
    } __attribute__((packed));

    static constexpr bool DEBUG = false;

    using TrieNodeDepth = TrieNodeIndex;

    Trie<NodeData, true> trie_;
    ankerl::unordered_dense::map<size_t, TrieNodeIndex> bucket_ends_; // maps frequencies to the end of the corresponding bucket
    size_t threshold_;

    size_t k_;

    static constexpr TrieNodeIndex NIL = 0;

    TrieNodeIndex prev(TrieNodeIndex const x) const ALWAYS_INLINE {
        // get the previous node in a circular manner, excluding the root
        return (x == 1) ? k_ - 1 : x - 1;
    }

    void decrement_all() ALWAYS_INLINE {
        // erase current threshold bucket if it exists
        bucket_ends_.erase(threshold_);

        // then simply increment the threshold
        ++threshold_;
    }

    TrieNodeIndex increment(TrieNodeIndex const i) ALWAYS_INLINE {
        // get frequency, assuring that it is >= threshold
        // i.e., nodes that are below threshold are artificially lifted up
        auto const f = std::max(trie_.node(i).freq, threshold_);

        // swap to end of the corresponding bucket, if necessary
        auto const j = bucket_ends_[f];
        if(j != i) {
            assert(trie_.node(j).freq == f);
            trie_.swap(i, j);
        }

        // update bucket
        auto const before_j = prev(j);
        if(trie_.node(before_j).freq == f) {
            // the previous node has the same frequency, move bucket pointer
            bucket_ends_[f] = before_j;
        } else {
            // the incremented node was the last in its bucket, erase the bucket
            bucket_ends_.erase(f);
        }

        // increment frequency
        trie_.node(j).freq = f + 1;
        if(i != j) assert(trie_.node(i).freq <= f);
        assert(trie_.node(j).freq > threshold_);

        // create bucket if needed
        if(!bucket_ends_.contains(f+1)) {
            bucket_ends_[f + 1] = j;
        }

        // return node's new index
        return j;
    }

    TrieNodeIndex insert(TrieNodeIndex const parent, char const label) ALWAYS_INLINE {
        if(bucket_ends_.contains(threshold_)) {
            // recycle something from the garbage
            auto const i = bucket_ends_[threshold_];

            // override node in trie
            trie_.extract(i);
            auto& data = trie_.insert_child(i, parent, label);

            // make sure the frequency is <= threshold
            data.freq = threshold_;

            // now simply increment
            return increment(i);
        } else {
            // the trie is full
            return NIL;
        }
    }

public:
    inline TopKPrefixesMisraGries2(size_t const k, size_t const sketch_columns, size_t const fp_window_size = 8)
        : trie_(k),
          k_(k),
          threshold_(0) {
        
        // initialize all k nodes as orphans in trie
        trie_.fill();

        // create initial garbage bucket that contains all nodes
        bucket_ends_.reserve(k);
        bucket_ends_[0] = k - 1;
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
            ext.node = increment(ext.node);
            assert(trie_.node(ext.node).freq > threshold_);

            // done
            ext.frequent = true;
        } else {
            // the current prefix is non-frequent

            // attempt to insert it
            ext.node = insert(s.node, c);
            if(ext.node == NIL) {
                // that failed, decrement everything else in turn
                decrement_all();
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
    }
};
