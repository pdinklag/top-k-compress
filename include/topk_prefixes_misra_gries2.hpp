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

        TrieNodeIndex pos; // the current positon in the Space-Saving array

        NodeData() {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c) {
        }
    } __attribute__((packed));

    struct NodeFrequency {
        TrieNodeIndex node;
        size_t freq;
    } __attribute__((packed));

    static constexpr bool DEBUG = false;

    using TrieNodeDepth = TrieNodeIndex;

    size_t k_;

    Trie<NodeData, true> trie_;
    std::unique_ptr<NodeFrequency[]> freqs_;                          // the circular Space-Saving array of frequencies
    ankerl::unordered_dense::map<size_t, TrieNodeIndex> bucket_ends_; // maps frequencies to the slot that marks the end of the corresponding buckets
    size_t threshold_;                                                // the current "garbage" bucket

    size_t num_slots() const ALWAYS_INLINE {
        return k_ - 1;
    }

    TrieNodeIndex prev_slot(TrieNodeIndex const x) const ALWAYS_INLINE {
        // get the previous slot in a circular manner
        return (x == 0) ? (num_slots() - 1) : (x - 1);
    }

    void decrement_all() ALWAYS_INLINE {
        // erase current threshold bucket if it exists
        bucket_ends_.erase(threshold_);

        // then simply increment the threshold
        ++threshold_;
    }

    void increment(TrieNodeIndex const v) ALWAYS_INLINE {
        // get position in the slots array
        auto const i = trie_.node(v).pos;
        assert(freqs_[i] == v);

        // get frequency, assuring that it is >= threshold
        // i.e., nodes that are below threshold are artificially lifted up
        auto const f = std::max(freqs_[i].freq, threshold_);
        assert(f + 1 > threshold_);

        // swap to end of the corresponding bucket, if necessary
        auto const it_f = bucket_ends_.find(f);
        assert(it_f != bucket_ends_.end());

        auto const j = it_f->second;
        if(j != i) {
            // swap
            auto const u = freqs_[j].node;
            assert(freqs_[j].freq == f);

            freqs_[j].node = v;
            freqs_[i].node = u;

            trie_.node(u).pos = i;
            trie_.node(v).pos = j;
        }
        assert(freqs_[j] == v);

        // update bucket
        auto const before_j = prev_slot(j);
        if(freqs_[before_j].freq == f) {
            // the node in the previous slot has the same frequency, move bucket pointer
            it_f->second = before_j;
        } else {
            // the incremented slot was the last in its bucket, erase the bucket
            bucket_ends_.erase(it_f);
        }

        // increment frequency
        freqs_[j].freq = f + 1;

        // create bucket if needed
        if(!bucket_ends_.contains(f+1)) {
            bucket_ends_.emplace(f+1, j);
        }
    }

    bool insert(TrieNodeIndex const parent, char const label, TrieNodeIndex& out_node) ALWAYS_INLINE {
        auto const it = bucket_ends_.find(threshold_);
        if(it != bucket_ends_.end()) {
            // recycle something from the garbage
            auto const i = it->second;
            assert(i < num_slots());

            // override node in trie
            auto const v = freqs_[i].node;
            trie_.extract(v);
            auto& data = trie_.insert_child(v, parent, label);

            // make sure the frequency is <= threshold
            freqs_[i].freq = threshold_;

            // map back to the position (insert_child overrode this)
            data.pos = i;

            // now simply increment
            increment(v);

            out_node = v;
            return true;
        } else {
            // there is no garbage to recycle
            out_node = trie_.root();
            return false;
        }
    }

public:
    inline TopKPrefixesMisraGries2(size_t const k, size_t const sketch_columns, size_t const fp_window_size = 8)
        : trie_(k),
          k_(k),
          threshold_(0) {
        
        // initialize all k nodes as orphans in trie
        trie_.fill();

        // initialize slots and enter all nodes except the root
        freqs_ = std::make_unique<NodeFrequency[]>(k-1);
        for(size_t i = 1; i < k; i++) {
            freqs_[i-1].node = i;
            freqs_[i-1].freq = 0;
        }

        // map trie nodes to slots
        for(size_t i = 1; i < k; i++) {
            trie_.node(i).pos = i-1;
        }

        // create initial garbage bucket that contains all nodes
        bucket_ends_.reserve(k);
        bucket_ends_[0] = num_slots() - 1;
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
            assert(trie_.node(ext.node).freq > threshold_);

            // done
            ext.frequent = true;
        } else {
            // the current prefix is non-frequent

            // attempt to insert it
            if(!insert(s.node, c, ext.node)) {
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
