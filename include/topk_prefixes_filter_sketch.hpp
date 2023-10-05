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
#include "count_min2.hpp"
#include "rolling_karp_rabin.hpp"
#include "display.hpp"

template<bool approx_minpq_ = false, std::unsigned_integral TrieNodeIndex = uint32_t>
class TopKPrefixesFilterSketch {
private:
    struct NodeData : public TrieNode<TrieNodeIndex> {
        using Character = TrieNode<TrieNodeIndex>::Character;
        using Index = TrieNode<TrieNodeIndex>::Index;

        size_t freq;                    // the current estimated frequency
        size_t insert_freq;             // the estimated frequency at time of insertion
        MinPQ<size_t>::Location minpq;  // the entry in the minimum PQ, if any
        uint64_t fingerprint;           // the 64-bit fingerprint of the represented string

        void dump_extra_info() const {
            std::cout << " [freq=" << freq << "]";
        }

        NodeData() {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c) {
        }
    } __attribute__((packed));

    static constexpr bool DEBUG = false;

    static constexpr uint64_t rolling_fp_offset_ = (1ULL << 63) - 25;
    static constexpr uint64_t rolling_fp_base_ = (1ULL << 14) - 15;    

    using TrieNodeDepth = TrieNodeIndex;

    Trie<NodeData> trie_;
    MinPQ<size_t, TrieNodeIndex> min_pq_;
    RollingKarpRabin hash_;
    CountMin2<size_t> sketch_;

    size_t k_;

    void increment_in_trie(TrieNodeIndex const v) ALWAYS_INLINE {
        // increment frequency
        auto& data = trie_.node(v);

        ++data.freq;
        if constexpr(approx_minpq_) {
            if(std::has_single_bit(data.freq) && trie_.is_leaf(v)) {
                // prefix is maximal frequent string, increase in min pq
                assert((bool)data.minpq);
                data.minpq = min_pq_.increase_key(data.minpq);
            }
        } else {
            if(trie_.is_leaf(v)) {
                // prefix is maximal frequent string, increase in min pq
                assert((bool)data.minpq);
                data.minpq = min_pq_.increase_key(data.minpq);
            }
        }
    }

    TrieNodeIndex insert_into_trie(TrieNodeIndex const parent, char const label, uint64_t const fingerprint) ALWAYS_INLINE {
        auto const v = trie_.new_node();
        trie_.insert_child(v, parent, label);

        // insert into min PQ as a maximal string
        auto& data = trie_.node(v);
        data.freq = 1;
        data.insert_freq = 0;
        data.fingerprint = fingerprint;
        data.minpq = min_pq_.insert(v, 1);

        assert(min_pq_.freq(data.minpq) == 1);

        // mark the parent no longer maximal
        auto& parent_data = trie_.node(parent);
        parent_data.minpq = min_pq_.remove(parent_data.minpq);

        return v;
    }

    TrieNodeIndex swap_into_trie(TrieNodeIndex const parent, char const label, uint64_t const fingerprint, size_t const frequency) ALWAYS_INLINE {
        // extract maximal frequent substring with minimal frequency
        TrieNodeIndex const swap = min_pq_.extract_min();

        // extract the substring from the trie and get the fingerprint and frequency delta
        assert(trie_.is_leaf(swap));
        auto const old_parent = trie_.extract(swap);
        auto& swap_data = trie_.node(swap);

        assert(swap_data.freq >= swap_data.insert_freq);
        auto const swap_freq_delta = swap_data.freq - swap_data.insert_freq;

        if(old_parent) {
            // propagate frequency delta to old parent (BEFORE potentially declaring it maximal)
            auto& old_parent_data = trie_.node(old_parent);
            old_parent_data.freq += swap_freq_delta;
        
            // the old parent may now be maximal
            if(old_parent_data.is_leaf()) {
                // insert into min PQ
                old_parent_data.minpq = min_pq_.insert(old_parent, approx_minpq_ ? std::bit_width(old_parent_data.freq) : old_parent_data.freq);
            }
        }

        // count the extracted string in the sketch as often as it had been counted in the trie
        sketch_.increment(swap_data.fingerprint, swap_freq_delta);

        // insert the current string into the trie, reusing the old node ID
        trie_.insert_child(swap, parent, label);

        swap_data.freq = frequency;
        swap_data.insert_freq = frequency;
        swap_data.fingerprint = fingerprint;
        assert(trie_.is_leaf(swap));

        // also insert it into the min PQ
        swap_data.minpq = min_pq_.insert(swap, approx_minpq_ ? std::bit_width(frequency) : frequency);
        
        // the parent is no longer maximal, remove from min PQ
        if(parent) {
            auto& parent_data = trie_.node(parent);
            parent_data.minpq = min_pq_.remove(parent_data.minpq);
        }

        return swap;
    }

public:
    inline TopKPrefixesFilterSketch(size_t const k, size_t const sketch_columns, size_t const fp_window_size = 8)
        : hash_(fp_window_size, rolling_fp_base_),
          trie_(k),
          min_pq_(k),
          sketch_(sketch_columns),
          k_(k) {
    }

    struct StringState {
        TrieNodeIndex len;         // length of the string
        TrieNodeIndex node;        // the string's node in the trie filter
        uint64_t      fingerprint; // fingerprint
        bool          frequent;    // whether or not the string is frequent
    };

    // returns a string state for the empty string to start with
    StringState empty_string() const ALWAYS_INLINE {
        StringState s;
        s.len = 0;
        s.node = trie_.root();
        s.fingerprint = rolling_fp_offset_;
        s.frequent = true;
        return s;
    }

    // extends a string to the right by a new character
    StringState extend(StringState const& s, char const c) ALWAYS_INLINE {
        auto const i = s.len;
        auto const ext_fp = hash_.push(s.fingerprint, c);

        // try and find extension in trie
        StringState ext;
        ext.len = i + 1;
        ext.fingerprint = ext_fp;

        bool const edge_exists = s.frequent && trie_.try_get_child(s.node, c, ext.node);
        if(edge_exists) {
            // the current prefix is frequent
            // we do not increment it in the trie directly, but do that lazily when we drop out of it
            // whenever a node is swapped out of the trie, the increment will be propagated

            // done
            ext.frequent = true;
        } else {
            // the current prefix is non-frequent

            // lazily increment immediate prefix in trie
            if(s.node) increment_in_trie(s.node);

            if(trie_.full()) {
                // the trie is full, count current prefix in the sketch
                // increment in sketch
                auto est = sketch_.increment_and_estimate<true>(ext_fp, 1);

                // test if it is now frequent
                auto const swap = (approx_minpq_ ? std::bit_width(est) : est) > min_pq_.min_frequency();
                if(swap) {
                    // it is now frequent according to just the numbers, test if we can swap
                    if(i == 0 || (s.node && trie_.node(s.node).freq >= est)) {
                        // the immediate prefix was frequent, so yes, we can!
                        ext.node = swap_into_trie(s.node, c, ext_fp, est);
                    } else {
                        // the immediate prefix was non-frequent or its frequency was too low
                        // -> the current prefix is overestimated, abort swap

                        // invalidate node
                        ext.node = 0;
                    }
                } else {
                    // nope, invalidate previous node
                    ext.node = 0;
                }
            } else {
                // insert into trie, which is not yet full
                ext.node = insert_into_trie(s.node, c, ext_fp);
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
    }
};
