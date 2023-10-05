/*
    DEPRECATED - USE THE SIMPLIFIED VARIANT
*/
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

#include "../always_inline.hpp"
#include "../trie.hpp"
#include "../truncated_trie.hpp"
#include "../min_pq.hpp"
#include "../count_min2.hpp"
#include "../rolling_karp_rabin.hpp"
#include "../display.hpp"

template<typename FilterNode, bool approx_minpq_ = false, size_t cut_ = 0>
requires requires {
    typename FilterNode::Index;
} && requires(FilterNode node) {
    { node.freq };
    { node.insert_freq };
    { node.fingerprint };
    { node.minpq };
}
class TopKSubstrings {
private:
    static constexpr bool gather_stats_ = false;
    static constexpr bool measure_time_ = false;
    static constexpr bool DEBUG = false;

    struct Stats {
        size_t num_strings_total;
        size_t num_filter_inc;
        size_t num_sketch_inc;
        size_t num_swaps;
        size_t num_overestimates;
        
        pm::Stopwatch t_filter_find;
        pm::Stopwatch t_filter_inc;
        pm::Stopwatch t_sketch_inc;
        pm::Stopwatch t_sketch_batch;
        pm::Stopwatch t_swaps;

        inline Stats() : num_strings_total(0),
                         num_filter_inc(0),
                         num_sketch_inc(0),
                         num_swaps(0),
                         num_overestimates(0) {
        }
    };

    static constexpr uint64_t rolling_fp_offset_ = (1ULL << 63) - 25;
    static constexpr uint64_t rolling_fp_base_ = (1ULL << 14) - 15;

    RollingKarpRabin hash_;

protected:
    using FilterIndex = typename FilterNode::Index;

private:
    static constexpr bool is_truncated_ = (cut_ > 1);
    using Filter = std::conditional_t<is_truncated_, TruncatedTrie<FilterNode, cut_>, Trie<FilterNode>>;

    Filter filter_;
    MinPQ<size_t, FilterIndex> min_pq_;
    CountMin2<size_t> sketch_;

    size_t k_;

    Stats stats_;

    void increment_in_filter(FilterIndex const v) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_filter_inc;
        if constexpr(measure_time_) stats_.t_filter_inc.resume();

        // increment frequency
        auto& data = filter_.node(v);

        ++data.freq;
        if constexpr(approx_minpq_) {
            if(std::has_single_bit(data.freq) && filter_.is_leaf(v)) {
                // prefix is maximal frequent string, increase in min pq
                assert((bool)data.minpq);
                data.minpq = min_pq_.increase_key(data.minpq);
            }
        } else {
            if(filter_.is_leaf(v)) {
                // prefix is maximal frequent string, increase in min pq
                assert((bool)data.minpq);
                data.minpq = min_pq_.increase_key(data.minpq);
            }
        }

        if constexpr(measure_time_) stats_.t_filter_inc.pause();
    }

    FilterIndex insert_into_filter(FilterIndex const parent, char const label, uint64_t const fingerprint, char const* context) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_filter_inc;

        auto const v = filter_.new_node();
        if constexpr(is_truncated_) {
            if(parent == filter_.root()) {
                filter_.insert_root_child(v, context);
            } else {
                filter_.insert_child(v, parent, label);
            }
        } else {
            filter_.insert_child(v, parent, label);
        }

        // insert into min PQ as a maximal string
        auto& data = filter_.node(v);
        data.freq = 1;
        data.insert_freq = 0;
        data.fingerprint = fingerprint;
        data.minpq = min_pq_.insert(v, 1);

        assert(min_pq_.freq(data.minpq) == 1);

        // mark the parent no longer maximal
        auto& parent_data = filter_.node(parent);
        parent_data.minpq = min_pq_.remove(parent_data.minpq);

        // callback
        if(on_filter_node_inserted) on_filter_node_inserted(v);

        return v;
    }

    FilterIndex swap_into_filter(FilterIndex const parent, char const label, uint64_t const fingerprint, size_t const frequency, char const* context) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_swaps;
        if constexpr(measure_time_) stats_.t_swaps.resume();

        // extract maximal frequent substring with minimal frequency
        FilterIndex const swap = min_pq_.extract_min();

        // extract the substring from the filter and get the fingerprint and frequency delta
        assert(filter_.is_leaf(swap));
        auto const old_parent = filter_.extract(swap);
        auto& swap_data = filter_.node(swap);

        assert(swap_data.freq >= swap_data.insert_freq);
        auto const swap_freq_delta = swap_data.freq - swap_data.insert_freq;

        if(old_parent) {
            // propagate frequency delta to old parent (BEFORE potentially declaring it maximal)
            auto& old_parent_data = filter_.node(old_parent);
            old_parent_data.freq += swap_freq_delta;
        
            // the old parent may now be maximal
            if(old_parent_data.is_leaf()) {
                // insert into min PQ
                old_parent_data.minpq = min_pq_.insert(old_parent, approx_minpq_ ? std::bit_width(old_parent_data.freq) : old_parent_data.freq);
            }
        }

        // count the extracted string in the sketch as often as it had been counted in the filter
        sketch_.increment(swap_data.fingerprint, swap_freq_delta);

        // callback
        if(on_delete_node) on_delete_node(swap);

        // insert the current string into the filter, reusing the old entries' node ID
        if constexpr(is_truncated_) {
            if(parent == filter_.root()) {
                filter_.insert_root_child(swap, context);
            } else {
                filter_.insert_child(swap, parent, label);
            }
        } else {
            filter_.insert_child(swap, parent, label);
        }

        swap_data.freq = frequency;
        swap_data.insert_freq = frequency;
        swap_data.fingerprint = fingerprint;
        assert(filter_.is_leaf(swap));

        // also insert it into the min PQ
        swap_data.minpq = min_pq_.insert(swap, approx_minpq_ ? std::bit_width(frequency) : frequency);
        
        // the parent is no longer maximal, remove from min PQ
        if(parent) {
            auto& parent_data = filter_.node(parent);
            parent_data.minpq = min_pq_.remove(parent_data.minpq);
        }

        // callback
        if(on_filter_node_inserted) on_filter_node_inserted(swap);
        
        if constexpr(measure_time_) stats_.t_swaps.pause();
        return swap;
    }

public:
    inline TopKSubstrings(size_t const k, size_t const sketch_rows, size_t const sketch_columns, size_t const fp_window_size = 8)
        : hash_(fp_window_size, rolling_fp_base_),
          filter_(k),
          min_pq_(k),
          sketch_(sketch_columns),
          k_(k),
          stats_() {
    }

    // callbacks
    std::function<void(FilterIndex const)> on_filter_node_inserted;
    std::function<void(FilterIndex const)> on_delete_node;

    struct StringState {
        FilterIndex len;         // length of the string
        FilterIndex node;        // the string's node in the filter
        uint64_t    fingerprint; // fingerprint
        char        first;       // the first ever character of this string
        bool        frequent;    // whether or not the string is frequent
        bool        new_node;    // did the last extension cause a new filter entry to be created?
        char        context[cut_];
    };

    // construct a string state for a specific node in the filter
    // the depth and root label of the node must be known, because such information is not stored by default
    StringState at(FilterIndex const node, FilterIndex const depth, char const first) ALWAYS_INLINE {
        if(!node) return empty_string(); // root

        StringState s;
        s.node = node;
        s.frequent = true;
        s.new_node = false;
        
        auto const* v = &filter_.node(node);
        s.fingerprint = v->fingerprint;

        s.len = depth;
        s.first = first;

        return s;
    }

    // returns a string state for the empty string to start with
    StringState empty_string() const ALWAYS_INLINE {
        StringState s;
        s.len = 0;
        s.node = filter_.root();
        s.fingerprint = rolling_fp_offset_;
        s.first = 0;
        s.frequent = true;
        s.new_node = false;
        return s;
    }

    // extends a string to the right by a new character
    // modifies the fingerprint window
    StringState extend(StringState const& s, char const c) ALWAYS_INLINE {
        auto const ext_fp = hash_.push(s.fingerprint, c);
        return extend(s, c, ext_fp);
    }

    // extends a string to the right by a new character
    // uses the passed fingerprint
    StringState extend(StringState const& s, char const c, uint64_t const ext_fp) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_strings_total;
        auto const i = s.len;

        // try and find extension in filter
        StringState ext;
        ext.len = i + 1;
        ext.fingerprint = ext_fp;
        ext.first = (i == 0) ? c : s.first;
        ext.new_node = false;

        bool edge_exists;
        if constexpr(is_truncated_) {
            // copy / extend context
            for(size_t j = 0; j < cut_; j++) {
                ext.context[j] = s.context[j];
            }
            if(s.len < cut_) ext.context[s.len] = c;

            if(ext.len < cut_) {
                // we are in the truncated level range, extend context and act as if we were frequent
                ext.frequent = true;
                ext.node = filter_.root();
                return ext;
            } else if (ext.len == cut_) {
                // we are now entering the actual trie node level range, find the entry point
                edge_exists = filter_.try_get_root_child(ext.context, ext.node);
            } else {
                // common case
                if constexpr(measure_time_) if(s.frequent) stats_.t_filter_find.resume();
                edge_exists = s.frequent && filter_.try_get_child(s.node, c, ext.node);
                if constexpr(measure_time_) stats_.t_filter_find.pause();
            }
        } else {
            if constexpr(measure_time_) if(s.frequent) stats_.t_filter_find.resume();
            edge_exists = s.frequent && filter_.try_get_child(s.node, c, ext.node);
            if constexpr(measure_time_) stats_.t_filter_find.pause();
        }

        if(edge_exists) {
            // the current prefix is frequent
            // we do not increment it in the filter directly, but do that lazily when we drop out of it
            // whenever a node is swapped out of the filter, the increment will be propagated

            // done
            ext.frequent = true;
        } else {
            // the current prefix is non-frequent

            // lazily increment immediate prefix in filter
            if(s.node) increment_in_filter(s.node);

            if(filter_.full()) {
                // the filter is full, count current prefix in the sketch
                if constexpr(gather_stats_) ++stats_.num_sketch_inc;

                // increment in sketch
                if constexpr(measure_time_) stats_.t_sketch_inc.resume();

                auto est = sketch_.increment_and_estimate(ext_fp, 1);
                if constexpr(measure_time_) stats_.t_sketch_inc.pause();

                // test if it is now frequent
                auto const swap = (approx_minpq_ ? std::bit_width(est) : est) > min_pq_.min_frequency();
                if(swap) {
                    // it is now frequent according to just the numbers, test if we can swap
                    if(i == 0 || (s.node && filter_.node(s.node).freq >= est)) {
                        // the immediate prefix was frequent, so yes, we can!
                        ext.node = swap_into_filter(s.node, c, ext_fp, est, ext.context);

                        // swapped in, so it's new
                        ext.new_node = true;
                    } else {
                        // the immediate prefix was non-frequent or its frequency was too low
                        // -> the current prefix is overestimated, abort swap
                        if constexpr(gather_stats_) ++stats_.num_overestimates;

                        // invalidate node
                        ext.node = 0;
                    }
                } else {
                    // nope, invalidate previous node
                    ext.node = 0;
                }
            } else {
                // insert into filter, which is not yet full
                ext.node = insert_into_filter(s.node, c, ext_fp, ext.context);
                ext.new_node = true;
            }

            // we dropped out of the filter, so no extension can be frequent (not even if the current prefix was inserted or swapped in)
            ext.frequent = false;
        }

        // advance
        if constexpr(DEBUG) {
            std::cout << "top-k: extend string of length " << s.len << " (node " << s.node << ") by " << display(c) << " -> node=" << ext.node << ", fp=" << ext.fingerprint << ", frequent=" << ext.frequent << std::endl;
        }

        return ext;
    }

    // forcefully drop out of the filter, causing an increment at the corresponding node if the current state is frequent
    void drop_out(StringState const& s) ALWAYS_INLINE {
        if(s.frequent && s.node) increment_in_filter(s.node);
    }

    size_t limit(StringState const& s, size_t const max_len) const {
        auto len = s.len;
        auto v = s.node;
        while(len > max_len) {
            v = filter_.parent(v);
            --len;
        }
        return v;
    }

    auto const& filter() const {
        return filter_;
    }

    FilterNode& filter_node(FilterIndex const node) {
        return filter_.node(node);
    }

    FilterNode const& filter_node(FilterIndex const node) const {
        return filter_.node(node);
    }

    size_t get(size_t const index, char* buffer) const {
        return filter_.spell(index, buffer);
    }


    void dump() const {
        std::vector<FilterIndex> nodes_by_freq;
        nodes_by_freq.reserve(filter_.size());
        for(size_t i = 1; i < filter_.size(); i++) {
            nodes_by_freq.emplace_back(i);
        }
        std::sort(nodes_by_freq.begin(), nodes_by_freq.end(), [&](auto a, auto b){
            return filter_node(a).freq > filter_node(b).freq;
        });

        auto buffer = std::make_unique<char[]>(1024);
        for(FilterIndex i : nodes_by_freq) {
            auto const d = filter_.spell(i, buffer.get());

            std::cout << i << ": \"";
            for(size_t j = 0; j < d; j++) std::cout << display_inline(buffer[j]);
            std::cout << "\" (" << d << ")";
            filter_node(i).dump_extra_info();
            std::cout << std::endl;
        }
    }

    void print_debug_info() const {
        if constexpr(gather_stats_) {
            std::cout << "top-k info"
                    << ": num_strings_total=" << stats_.num_strings_total
                    << ", num_filter_inc=" << stats_.num_filter_inc
                    << ", num_sketch_inc=" << stats_.num_sketch_inc
                    << ", num_swaps=" << stats_.num_swaps
                    << ", num_overestimates=" << stats_.num_overestimates;
            
            if constexpr(measure_time_) {
                std::cout << ", t_filter_find=" << (stats_.t_filter_find.elapsed_time_millis() / 1000.0)
                        << ", t_filter_inc=" << (stats_.t_filter_inc.elapsed_time_millis() / 1000.0)
                        << ", t_sketch_inc=" << (stats_.t_sketch_inc.elapsed_time_millis() / 1000.0)
                        << ", t_swaps=" << (stats_.t_swaps.elapsed_time_millis() / 1000.0);
            }
            
            std::cout << std::endl;
        }

        filter_.print_debug_info();
        min_pq_.print_debug_info();
    }
};
