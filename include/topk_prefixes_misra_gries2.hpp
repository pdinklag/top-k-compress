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
    static constexpr bool gather_stats_ = true;

    struct NodeData : public TrieNode<TrieNodeIndex> {
        using Character = TrieNode<TrieNodeIndex>::Character;
        using Index = TrieNode<TrieNodeIndex>::Index;

        TrieNodeIndex freq; // the current frequency
        TrieNodeIndex prev; // the previous node in frequency order
        TrieNodeIndex next; // the next node in frequency order

        NodeData() {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c) {
        }
    } __attribute__((packed));

    using TrieNodeDepth = TrieNodeIndex;

    static constexpr TrieNodeIndex BUCKET_DOES_NOT_EXIST = -1;
    static constexpr size_t renorm_divisor_ = 2;

    size_t k_;
    size_t max_allowed_frequency_;

    Trie<NodeData, true> trie_;
    std::unique_ptr<TrieNodeIndex[]> bucket_ends_; // maps frequencies to the last node in the corresponding bucket
    TrieNodeIndex threshold_;                      // the current "garbage" frequency

    // stats
    struct Stats {
        TrieNodeIndex max_freq = 0;
        size_t num_increment = 0;
        size_t num_increment_leaf = 0;
        size_t num_swap = 0;
        size_t num_decrement = 0;
        size_t num_renormalize = 0;
    } stats_;

    void renormalize() {
        if constexpr(gather_stats_) ++stats_.num_renormalize;

        // we normalize the frequency to [0, renorm_divisor_ * max_allowed_frequency_]
        // for this, we first divide every frequency by renorm_divisor_,
        // then we substract threshold_ / renorm_divisor_
        auto const adjusted_threshold = threshold_ / renorm_divisor_;
        auto renormalize = [&](size_t const f){ return f / renorm_divisor_ - adjusted_threshold; };

        for(size_t i = 1; i < k_; i++) {
            auto& data = trie_.node(i);
            auto const f = std::max(data.freq, threshold_); // nb: we must NOT allow frequency below the threshold, that would cause negative frequencies
            data.freq = renormalize(f);
        }

        // compact buckets
        for(size_t f = 0; f <= max_allowed_frequency_; f++) {
            if(bucket_ends_[f] != BUCKET_DOES_NOT_EXIST) {
                // we may update the bucket end for an adjusted frequency multiple times because we do integer division
                // however, we update in the order of frequencies, so bucket ends will correctly end up to be "rightmost" (in a circular manner)
                auto const adjusted_f = renormalize(f);
                bucket_ends_[adjusted_f] = bucket_ends_[f];
                bucket_ends_[f] = BUCKET_DOES_NOT_EXIST;
            }
        }

        // reset threshold
        threshold_ = 0;
    }

    void decrement_all() ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_decrement;

        // erase current threshold bucket if it exists
        bucket_ends_[threshold_] = BUCKET_DOES_NOT_EXIST;

        // then simply increment the threshold
        ++threshold_;
    }

    void swap_neighbours(TrieNodeIndex const u, TrieNodeIndex const v) ALWAYS_INLINE {
        auto& udata = trie_.node(u);
        auto& vdata = trie_.node(v);

        // u is predecessor of v ("w.l.o.g.")
        assert(udata.next == v);
        assert(vdata.prev == u);

        auto const x = udata.prev;
        auto const w = vdata.next;
        
        auto& xdata = trie_.node(x);
        auto& wdata = trie_.node(w);

        // sanity
        assert(xdata.next == u);
        assert(wdata.prev == v);

        // swap
        xdata.next = v;
        udata.prev = v;
        udata.next = w;
        vdata.prev = x;
        vdata.next = u;
        wdata.prev = u;
    }

    void swap(TrieNodeIndex const u, TrieNodeIndex const v) ALWAYS_INLINE {
        assert(u != v);

        auto& udata = trie_.node(u);
        auto& vdata = trie_.node(v);

        auto const x = udata.prev;
        auto const y = udata.next;
        auto const z = vdata.prev;
        auto const w = vdata.next;

        if(x == v) {
            swap_neighbours(v, u);
        } else if(y == v) {
            swap_neighbours(u, v);
        } else {
            auto& xdata = trie_.node(x);
            auto& ydata = trie_.node(y);
            auto& zdata = trie_.node(z);
            auto& wdata = trie_.node(w);

            // sanity
            assert(xdata.next == u);
            assert(ydata.prev == u);
            assert(zdata.next == v);
            assert(wdata.prev == v);

            // swap
            xdata.next = v;
            udata.prev = z;
            udata.next = w;
            ydata.prev = v;
            zdata.next = u;
            vdata.prev = x;
            vdata.next = y;
            wdata.prev = u;
        }
    }

    void increment(TrieNodeIndex const v) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_increment;

        // get frequency, assuring that it is >= threshold
        // i.e., nodes that are below threshold are artificially lifted up
        auto& vdata = trie_.node(v);
        auto const f = std::max(vdata.freq, threshold_);
        assert(f + 1 > threshold_);
        if constexpr(gather_stats_) {
            if(vdata.is_leaf()) ++stats_.num_increment_leaf;
        }

        // swap to end of the corresponding bucket, if necessary
        auto const u = bucket_ends_[f];
        assert(u != BUCKET_DOES_NOT_EXIST);
        if(u != v) {
            if constexpr(gather_stats_) ++stats_.num_swap;

            swap(v, u);
        }

        // update bucket
        auto const prev = vdata.prev;
        if(trie_.node(prev).freq == f) {
            // the previous node has the same frequency, move bucket pointer
            bucket_ends_[f] = prev;
        } else {
            // the incremented node was the last in its bucket, erase the bucket
            bucket_ends_[f] = BUCKET_DOES_NOT_EXIST;
        }

        // increment frequency
        vdata.freq = f + 1;
        if constexpr(gather_stats_) stats_.max_freq = std::max(f+1, stats_.max_freq);

        // create bucket if needed
        if(bucket_ends_[f+1] == BUCKET_DOES_NOT_EXIST) {
            bucket_ends_[f+1] = v;
        }

        // possibly renormalize
        if(f + 1 == max_allowed_frequency_) {
            if constexpr(gather_stats_) ++stats_.num_renormalize;
            renormalize();
        }
    }

    bool insert(TrieNodeIndex const parent, char const label, TrieNodeIndex& out_node) ALWAYS_INLINE {
        auto const v = bucket_ends_[threshold_];
        if(v != BUCKET_DOES_NOT_EXIST) {
            // recycle something from the garbage
            assert(v < k_);
            assert(v != 0);

            // override node in trie
            auto& data = trie_.node(v);
            auto const prev = data.prev;
            auto const next = data.next;

            trie_.extract(v);
            trie_.insert_child(v, parent, label);

            // make sure the frequency is <= threshold
            data.freq = threshold_;

            // write back data that was overrridden by insert_child
            data.prev = prev;
            data.next = next;

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
          max_allowed_frequency_(sketch_columns-1),
          threshold_(0) {
        
        if(sketch_columns == 0) {
            std::cerr << "need at least one column" << std::endl;
            std::abort();
        }

        // initialize all k nodes as orphans in trie
        trie_.fill();

        // cyclically link trie nodes in frequency order, EXCLUDING the root
        for(size_t i = 1; i < k; i++) {
            auto& data = trie_.node(i);
            data.prev = (i > 1)     ? (i - 1) : (k - 1);
            data.next = (i < k - 1) ? (i + 1) : 1;
        }

        // create initial garbage bucket that contains all nodes
        bucket_ends_ = std::make_unique<TrieNodeIndex[]>(max_allowed_frequency_ + 1);
        bucket_ends_[0] = k - 1;
        for(size_t i = 1; i <= max_allowed_frequency_; i++) {
            bucket_ends_[i] = BUCKET_DOES_NOT_EXIST;
        }
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

            // attempt to insert it
            if(!insert(s.node, c, ext.node)) {
                // that failed, decrement everything else in turn
                decrement_all();
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

        std::cout << "# DEBUG: misra-gries2" << ", threshold=" << threshold_;
        if constexpr(gather_stats_) {
            std::cout
                << ", max_freq=" << stats_.max_freq
                << ", num_increment=" << stats_.num_increment
                << ", num_increment_leaf=" << stats_.num_increment_leaf
                << ", num_swap=" << stats_.num_swap
                << ", num_decrement=" << stats_.num_decrement
                << ", num_renormalize=" << stats_.num_renormalize
                ;
        } else {
            std::cout << " (advanced stats disabled)";
        }

        std::cout << std::endl;
    }
};
