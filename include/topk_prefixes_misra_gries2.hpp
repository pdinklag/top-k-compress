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

    static constexpr TrieNodeIndex NIL = -1;

    struct NodeData : public TrieNode<TrieNodeIndex> {
        using Character = TrieNode<TrieNodeIndex>::Character;
        using Index = TrieNode<TrieNodeIndex>::Index;

        TrieNodeIndex freq; // the current frequency
        TrieNodeIndex prev; // the previous node in frequency order
        TrieNodeIndex next; // the next node in frequency order

        NodeData() {
        }

        NodeData(Index v, Character c) : TrieNode<TrieNodeIndex>(v, c), freq(0), prev(NIL), next(NIL) {
        }
    } __attribute__((packed));

    using TrieNodeDepth = TrieNodeIndex;

    static constexpr size_t renorm_divisor_ = 2;

    size_t k_;
    size_t max_allowed_frequency_;

    Trie<NodeData, true> trie_;
    std::unique_ptr<TrieNodeIndex[]> bucket_head_; // maps frequencies to the head of the corresponding linked list of leaves
    TrieNodeIndex threshold_;                      // the current "garbage" frequency

    // stats
    struct Stats {
        TrieNodeIndex max_freq = 0;
        size_t num_increment = 0;
        size_t num_increment_leaf = 0;
        size_t num_decrement = 0;
        size_t num_tail_search = 0;
        size_t num_renormalize = 0;
    } stats_;

    void preprend_list(TrieNodeIndex const old_head, TrieNodeIndex const new_head) {
        if(old_head != NIL) {
            // find tail of preprended list
            if constexpr(gather_stats_) ++stats_.num_tail_search;
            auto link = new_head;
            while(trie_.node(link).next != NIL) {
                link = trie_.node(link).next;
            }

            // re-chain contents
            auto& link_data = trie_.node(link);
            link_data.next = old_head;

            auto& old_head_data = trie_.node(old_head);
            assert(old_head_data == NIL);
            old_head_data.prev = link;
        }
    }

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
        auto compacted_buckets = std::make_unique<TrieNodeIndex[]>(max_allowed_frequency_ + 1);
        for(size_t f = 0; f <= max_allowed_frequency_; f++) {
            auto const head = bucket_head_[f];
            if(head != NIL) {
                auto const adjusted_f = renormalize(f);
                preprend_list(compacted_buckets[adjusted_f], head);
                compacted_buckets[adjusted_f] = head;
            }
        }
        bucket_head_ = std::move(compacted_buckets);

        // reset threshold
        threshold_ = 0;
    }

    void decrement_all() ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_decrement;

        // if current threshold bucket exists, prepend all its nodes to the next bucket
        auto const head = bucket_head_[threshold_];
        if(head != NIL) {
            // prepend all to next bucket
            preprend_list(bucket_head_[threshold_ + 1], head);
            bucket_head_[threshold_ + 1] = head;

            // delete threshold bucket
            bucket_head_[threshold_] = NIL;
        }

        // then simply increment the threshold
        ++threshold_;
    }

    void unlink(TrieNodeIndex const v) ALWAYS_INLINE {
        auto& vdata = trie_.node(v);

        // remove
        auto const x = vdata.prev;
        auto const y = vdata.next;

        vdata.prev = NIL;
        vdata.next = NIL;

        if(x != NIL) {
            auto& xdata = trie_.node(x);
            xdata.next = y;
        }

        if(y != NIL) {
            auto& ydata = trie_.node(y);
            ydata.prev = x;
        }

        // if v was the head of its bucket, update the bucket
        auto const f = vdata.freq;
        if(bucket_head_[f] == v) {
            assert(x == NIL);

            // move bucket head to next (maybe NIL)
            bucket_head_[f] = y;
        }
    }

    void link(TrieNodeIndex const v) ALWAYS_INLINE {
        assert(trie_.is_valid_nonroot(v));
        auto& vdata = trie_.node(v);
        assert(vdata.is_leaf());

        auto const f = std::max(vdata.freq, threshold_); // make sure frequency is at least threshold

        // make new head of bucket
        auto const u = bucket_head_[f];
        if(u != NIL) {
            auto& udata = trie_.node(u);
            assert(udata.prev == NIL);

            vdata.next = u;
            udata.prev = v;
        }
        bucket_head_[f] = v;
    }

    void increment(TrieNodeIndex const v) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_increment;

        // get frequency, assuring that it is >= threshold
        auto& vdata = trie_.node(v);
        auto const f = std::max(vdata.freq, threshold_);

        if(vdata.is_leaf()) {
            if constexpr(gather_stats_) ++stats_.num_increment_leaf;

            // unlink from wherever it is currently linked at
            unlink(v);

            // re-link as head of next bucket
            auto const u = bucket_head_[f+1];
            if(u != NIL) {
                auto& udata = trie_.node(u);
                assert(udata.prev == NIL);

                vdata.next = u;
                udata.prev = v;
            }
            bucket_head_[f+1] = v;
        }

        // increment frequency
        vdata.freq = f + 1;
        if constexpr(gather_stats_) stats_.max_freq = std::max(f+1, stats_.max_freq);

        // possibly renormalize
        if(f + 1 == max_allowed_frequency_) {
            if constexpr(gather_stats_) ++stats_.num_renormalize;
            renormalize();
        }
    }

    bool insert(TrieNodeIndex const parent, char const label, TrieNodeIndex& out_node) ALWAYS_INLINE {
        auto const v = bucket_head_[threshold_];
        if(v != NIL) {
            // recycle something from the garbage
            assert(v < k_);
            assert(v != 0);

            auto& vdata = trie_.node(v);
            assert(vdata.is_leaf());
            assert(vdata.freq <= threshold_);

            // extract from trie
            auto const old_parent = trie_.extract(v);

            // old parent may have become a leaf
            if(trie_.is_valid_nonroot(old_parent) && trie_.is_leaf(old_parent)) {
                link(old_parent);
            }

            // new parent can no longer be a leaf
            unlink(parent);

            // insert into trie with new parent
            auto const prev = vdata.prev;
            auto const next = vdata.next;
            trie_.insert_child(v, parent, label);

            // write back data that was overrridden by insert_child
            vdata.prev = prev;
            vdata.next = next;

            // make sure the frequency is >= threshold
            vdata.freq = threshold_;

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

        // link trie nodes in garbage bucket
        for(size_t i = 1; i < k; i++) {
            auto& data = trie_.node(i);
            data.prev = (i > 1)     ? (i - 1) : NIL;
            data.next = (i < k - 1) ? (i + 1) : NIL;
        }

        // create initial garbage bucket that contains all nodes
        bucket_head_ = std::make_unique<TrieNodeIndex[]>(max_allowed_frequency_ + 1);
        bucket_head_[0] = 1;
        for(size_t i = 1; i <= max_allowed_frequency_; i++) {
            bucket_head_[i] = NIL;
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
                << ", num_decrement=" << stats_.num_decrement
                << ", num_tail_search=" << stats_.num_tail_search
                << ", num_renormalize=" << stats_.num_renormalize
                ;
        } else {
            std::cout << " (advanced stats disabled)";
        }

        std::cout << std::endl;
    }
};
