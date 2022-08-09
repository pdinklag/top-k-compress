#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>

#include <pm/stopwatch.hpp>
#include <tdc/util/concepts.hpp>

#include "always_inline.hpp"
#include "trie.hpp"
#include "min_pq.hpp"
#include "count_min.hpp"
#include "rolling_karp_rabin.hpp"

class TopKSubstrings {
private:
    static constexpr bool gather_stats_ = true;
    static constexpr bool measure_time_ = false;
    
    struct Stats {
        size_t num_strings_total;
        size_t num_filter_inc;
        size_t num_sketch_inc;
        size_t num_swaps;
        size_t num_overestimates;
        
        pm::Stopwatch t_filter_find;
        pm::Stopwatch t_filter_inc;
        pm::Stopwatch t_sketch_inc;
        pm::Stopwatch t_swaps;

        inline Stats() : num_strings_total(0),
                         num_filter_inc(0),
                         num_sketch_inc(0),
                         num_swaps(0),
                         num_overestimates(0) {
        }
    };

    static constexpr size_t hash_window_size_ = 8;

    static constexpr uint64_t rolling_fp_offset_ = (1ULL << 63) - 25;
    static constexpr uint64_t rolling_fp_base_ = (1ULL << 14) - 15;

    struct FilterNodeData {
        size_t freq;
        size_t insert_freq;
        uint64_t fingerprint;
        MinPQ<size_t>::Location minpq;
    } __attribute__((packed));

    RollingKarpRabin hash_;
    std::unique_ptr<char[]> hash_window_;

    using FilterIndex = uint32_t;
    Trie<FilterNodeData, FilterIndex> filter_;
    MinPQ<size_t, FilterIndex> min_pq_;
    CountMin<size_t> sketch_;

    size_t k_;
    size_t len_;

    Stats stats_;

public:
    inline TopKSubstrings(size_t const k, size_t const len, size_t sketch_rows, size_t sketch_columns)
    : hash_(hash_window_size_, rolling_fp_base_),
      hash_window_(std::make_unique<char[]>(len + 1)), // +1 is just for debugging purposes...
      filter_(k),
      min_pq_(k),
      sketch_(sketch_rows, sketch_columns),
      k_(k),
      len_(len),
      stats_() {
    }

    struct StringState {
        FilterIndex len;         // length of the string
        FilterIndex node;        // the string's node in the filter
        uint64_t    fingerprint; // fingerprint
        bool        frequent;    // whether or not the string is frequent
    };

    // returns a string state for the empty string to start with
    // also conceptually clears the fingerprint window
    StringState empty_string() ALWAYS_INLINE {
        return StringState { 0, filter_.root(), rolling_fp_offset_, true };
    }

    // extends a string to the right by a new character
    // modifies the fingerprint window
    StringState extend(StringState const& s, char const c) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_strings_total;
        auto const i = s.len;

        // update fingerprint
        hash_window_[i] = c;
        char const pop = (i >= hash_window_size_) ? hash_window_[i - hash_window_size_] : 0;
        auto const ext_fp = hash_.roll(s.fingerprint, pop, c);

        // try and find extension in filter
        StringState ext;
        ext.len = i + 1;
        ext.fingerprint = ext_fp;

        if constexpr(measure_time_) if(s.frequent) stats_.t_filter_find.resume();
        if(s.frequent && filter_.try_get_child(s.node, c, ext.node)) {
            auto const ext_node = ext.node;
            if constexpr(measure_time_) stats_.t_filter_find.pause();
            
            // the current prefix is frequent
            if constexpr(gather_stats_) ++stats_.num_filter_inc;
            if constexpr(measure_time_) stats_.t_filter_inc.resume();

            // increment frequency
            bool const maximal = filter_.is_leaf(ext_node);
            auto& data = filter_.data(ext_node);
            ++data.freq;
            if(maximal) {
                // prefix is maximal frequent string, increase in min pq
                assert((bool)data.minpq);
                data.minpq = min_pq_.increase_key(data.minpq);
            }

            if constexpr(measure_time_) stats_.t_filter_inc.pause();

            // done
            ext.frequent = true;
        } else {
            if constexpr(measure_time_) if(s.frequent) stats_.t_filter_find.pause();
            
            // the current prefix is non-frequent
            if(filter_.full()) {
                // the filter is full, count current prefix in the sketch
                if constexpr(gather_stats_) ++stats_.num_sketch_inc;
                
                if constexpr(measure_time_) stats_.t_sketch_inc.resume();
                auto est = sketch_.increment_and_estimate(ext_fp);
                if constexpr(measure_time_) stats_.t_sketch_inc.pause();

                // test if it is now frequent
                if(est > min_pq_.min_frequency()) {
                    // it is now frequent according to just the numbers, test if we can swap
                    if((s.node && filter_.data(s.node).freq >= est) || i == 0) {
                        // std::cout << "swap! est=" << est << ", min=" << min_pq_.min_frequency() << std::endl;

                        // the immediate prefix was frequent, so yes, we can!
                        if constexpr(gather_stats_) ++stats_.num_swaps;
                        if constexpr(measure_time_) stats_.t_swaps.resume();

                        // extract maximal frequent substring with minimal frequency
                        size_t const swap = min_pq_.extract_min();

                        // extract the substring from the filter and get the fingerprint and frequency delta
                        assert(filter_.is_leaf(swap));
                        auto const parent = filter_.extract(swap);
                        auto& swap_data = filter_.data(swap);

                        // the parent may now be maximal
                        if(parent && filter_.is_leaf(parent)) {
                            // insert into min PQ
                            auto& parent_data = filter_.data(parent);
                            parent_data.minpq = min_pq_.insert(parent, parent_data.freq);
                            assert(min_pq_.freq(parent_data.minpq) == parent_data.freq);
                        }

                        // count the extracted string in the sketch as often as it had been counted in the filter
                        assert(swap_data.freq >= swap_data.insert_freq);
                        sketch_.increment(swap_data.fingerprint, swap_data.freq - swap_data.insert_freq);

                        // insert the current string into the filter, reusing the old entries' node ID
                        filter_.insert_child(swap, s.node, c);
                        filter_.data(swap) = FilterNodeData { est, est, ext_fp };
                        assert(filter_.is_leaf(swap));

                        // also insert it into the min PQ
                        swap_data.minpq = min_pq_.insert(swap, est);
                        assert(min_pq_.freq(swap_data.minpq) == est);
                        
                        // the previous prefix is no longer maximal, remove from min PQ
                        if(s.node) {
                            auto& previous_data = filter_.data(s.node);
                            previous_data.minpq = min_pq_.remove(previous_data.minpq);
                        }

                        // make new node the extensions' node
                        ext.node = swap;
                        
                        if constexpr(measure_time_) stats_.t_swaps.pause();
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
                if constexpr(gather_stats_) ++stats_.num_filter_inc;

                ext.node = filter_.new_node();
                filter_.insert_child(ext.node, s.node, c);
                filter_.data(ext.node) = FilterNodeData { 1, 1, ext.fingerprint, min_pq_.insert(ext.node, 1) };

                // insert into min PQ as a maximal string
                assert(min_pq_.freq(filter_.data(ext.node).minpq) == 1);

                // mark the immediate prefix of this string no longer maximal
                auto& previous_data = filter_.data(s.node);
                previous_data.minpq = min_pq_.remove(previous_data.minpq);
            }

            // we dropped out of the filter, so no extension can be frequent (not even if the current prefix was inserted or swapped in)
            ext.frequent = false;
        }

        // advance
        return ext;
    }

    size_t get(size_t const index, char* buffer) const {
        return filter_.spell(index, buffer);
    }

    void print_debug_info() const {
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
        min_pq_.print_debug_info();
        sketch_.print_debug_info();
        /*
        char buffer[len_ + 1];
        for(size_t i = 1; i < k_; i++) {
            auto const l = get(i, buffer);
            buffer[l] = 0;
            std::cout << i << " -> \"" << buffer << "\" (" << filter_.freq(i) << ")" << std::endl;
        }
        */
        // sketch_.print_debug_info();
    }
};
