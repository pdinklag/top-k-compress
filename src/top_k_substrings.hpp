#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <vector>

#include <ankerl/unordered_dense.h>
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
    static constexpr bool sketch_batching_ = true;

    static constexpr size_t sketch_batch_size_ = 100;
    
    struct Stats {
        size_t num_strings_total;
        size_t num_filter_inc;
        size_t num_sketch_inc;
        size_t num_sketch_batches;
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
                         num_sketch_batches(0),
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

    struct SketchQueueEntry {
        FilterIndex parent;
        size_t count;
        char label;
        bool single_letter;

        inline SketchQueueEntry() {
        }

        inline SketchQueueEntry(FilterIndex const _parent, char const _label, bool _single_letter)
            : parent(_parent), count(1), label(_label), single_letter(_single_letter) {
        }
    };

    ankerl::unordered_dense::map<uint64_t, SketchQueueEntry> sketch_queue_;
    size_t sketch_count_;
    ankerl::unordered_dense::set<size_t> sketch_swapped_away_;
    std::unique_ptr<decltype(sketch_)::BatchWorkItem[]> sketch_work_;

    size_t k_;
    size_t len_;

    Stats stats_;

    void increment_in_filter(FilterIndex const v) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_filter_inc;
        if constexpr(measure_time_) stats_.t_filter_inc.resume();

        // increment frequency
        bool const maximal = filter_.is_leaf(v);
        auto& data = filter_.data(v);
        ++data.freq;
        if(maximal) {
            // prefix is maximal frequent string, increase in min pq
            assert((bool)data.minpq);
            data.minpq = min_pq_.increase_key(data.minpq);
        }

        if constexpr(measure_time_) stats_.t_filter_inc.pause();
    }

    FilterIndex insert_into_filter(FilterIndex const parent, char const label, uint64_t const fingerprint) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_filter_inc;

        auto const v = filter_.new_node();
        filter_.insert_child(v, parent, label);

        // insert into min PQ as a maximal string
        filter_.data(v) = FilterNodeData { 1, 1, fingerprint, min_pq_.insert(v, 1) };
        assert(min_pq_.freq(filter_.data(v).minpq) == 1);

        // mark the parent no longer maximal
        auto& parent_data = filter_.data(parent);
        parent_data.minpq = min_pq_.remove(parent_data.minpq);

        return v;
    }

    FilterIndex swap_into_filter(FilterIndex const parent, char const label, uint64_t const fingerprint, size_t const frequency) ALWAYS_INLINE {
        if constexpr(gather_stats_) ++stats_.num_swaps;
        if constexpr(measure_time_) stats_.t_swaps.resume();

        // extract maximal frequent substring with minimal frequency
        FilterIndex const swap = min_pq_.extract_min();

        // extract the substring from the filter and get the fingerprint and frequency delta
        assert(filter_.is_leaf(swap));
        auto const old_parent = filter_.extract(swap);
        auto& swap_data = filter_.data(swap);

        // the old parent may now be maximal
        if(old_parent && filter_.is_leaf(old_parent)) {
            // insert into min PQ
            auto& old_parent_data = filter_.data(old_parent);
            old_parent_data.minpq = min_pq_.insert(old_parent, old_parent_data.freq);
            assert(min_pq_.freq(old_parent_data.minpq) == old_parent_data.freq);
        }

        // count the extracted string in the sketch as often as it had been counted in the filter
        assert(swap_data.freq >= swap_data.insert_freq);
        sketch_.increment(swap_data.fingerprint, swap_data.freq - swap_data.insert_freq);

        // insert the current string into the filter, reusing the old entries' node ID
        filter_.insert_child(swap, parent, label);
        filter_.data(swap) = FilterNodeData { frequency, frequency, fingerprint };
        assert(filter_.is_leaf(swap));

        // also insert it into the min PQ
        swap_data.minpq = min_pq_.insert(swap, frequency);
        assert(min_pq_.freq(swap_data.minpq) == frequency);
        
        // the parent is no longer maximal, remove from min PQ
        if(parent) {
            auto& parent_data = filter_.data(parent);
            parent_data.minpq = min_pq_.remove(parent_data.minpq);
        }
        
        if constexpr(measure_time_) stats_.t_swaps.pause();
        return swap;
    }

    void process_sketch_queue() {
        if constexpr(gather_stats_) ++stats_.num_sketch_batches;
        if constexpr(measure_time_) stats_.t_sketch_batch.resume();

        sketch_swapped_away_.clear();

        // batch sketch
        size_t const batch_num = sketch_queue_.size();
        size_t i = 0;
        for(auto const& it : sketch_queue_) {
            sketch_work_[i].item = it.first;
            sketch_work_[i].inc = it.second.count;
            ++i;
        }

        sketch_.batch_increment_and_estimate(sketch_work_.get(), batch_num);

        // test for swaps
        size_t min_freq = min_pq_.min_frequency();
        for(size_t i = 0; i < batch_num; i++) {
            auto const est = sketch_work_[i].est;
            if(est > min_freq) {
                // this string may now be frequent, we must make sure of three things:
                // (1) the parent was frequent at queue time
                // (2) the parent is still frequent right now (we are processing a batch, so that info may be outdated)
                // (3) the parent's frequency is higher than the estimate (otherwise we are clearly dealing with an overestimate)
                auto const fp = sketch_work_[i].item;
                auto const& e = sketch_queue_.find(fp)->second; // nb: must exist
                if(e.parent && !sketch_swapped_away_.contains(e.parent) && (filter_.data(e.parent).freq >= est || e.single_letter)) {
                    auto const swapped = swap_into_filter(e.parent, e.label, fp, est);
                    min_freq = min_pq_.min_frequency();
                    sketch_swapped_away_.insert(swapped);
                } else {
                    if constexpr(gather_stats_) ++stats_.num_overestimates;
                }
            }
        }

        if constexpr(measure_time_) stats_.t_sketch_batch.pause();
    }

public:
    inline TopKSubstrings(size_t const k, size_t const len, size_t sketch_rows, size_t sketch_columns)
        : hash_(hash_window_size_, rolling_fp_base_),
          hash_window_(std::make_unique<char[]>(len + 1)), // +1 is just for debugging purposes...
          filter_(k),
          min_pq_(k),
          sketch_(sketch_rows, sketch_columns),
          sketch_count_(0),
          k_(k),
          len_(len),
          stats_() {

        if constexpr(sketch_batching_) {
            sketch_queue_.reserve(sketch_batch_size_);
            sketch_work_ = std::make_unique<decltype(sketch_)::BatchWorkItem[]>(sketch_batch_size_);
        }
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
            if constexpr(measure_time_) stats_.t_filter_find.pause();
            
            // the current prefix is frequent, increment it in the filter
            increment_in_filter(ext.node);

            // done
            ext.frequent = true;
        } else {
            if constexpr(measure_time_) if(s.frequent) stats_.t_filter_find.pause();
            
            // the current prefix is non-frequent
            if(filter_.full()) {
                // the filter is full, count current prefix in the sketch
                if constexpr(gather_stats_) ++stats_.num_sketch_inc;

                if constexpr(sketch_batching_) {
                    // enter into skech queue
                    auto it = sketch_queue_.find(ext_fp);
                    if(it != sketch_queue_.end()) {
                        ++it->second.count;
                    } else {
                        sketch_queue_.emplace(ext_fp, SketchQueueEntry(s.node, c, i == 0));
                    }

                    // if sketch queue is full, sketch as batch
                    if(++sketch_count_ >= sketch_batch_size_) {
                        process_sketch_queue();
                        sketch_queue_.clear();
                        sketch_count_ = 0;
                    }

                    // invalidate node
                    ext.node = 0;
                } else {
                    if constexpr(measure_time_) stats_.t_sketch_inc.resume();
                    auto est = sketch_.increment_and_estimate(ext_fp);
                    if constexpr(measure_time_) stats_.t_sketch_inc.pause();

                    // test if it is now frequent
                    if(est > min_pq_.min_frequency()) {
                        // it is now frequent according to just the numbers, test if we can swap
                        if((s.node && filter_.data(s.node).freq >= est) || i == 0) {
                            // the immediate prefix was frequent, so yes, we can!
                            ext.node = swap_into_filter(s.node, c, ext_fp, est);
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
                }
            } else {
                // insert into filter, which is not yet full
                ext.node = insert_into_filter(s.node, c, ext_fp);
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

        if constexpr(sketch_batching_) {
            std::cout << ", num_sketch_batches=" << stats_.num_sketch_batches;
        }

        if constexpr(measure_time_) {
            std::cout << ", t_filter_find=" << (stats_.t_filter_find.elapsed_time_millis() / 1000.0)
                      << ", t_filter_inc=" << (stats_.t_filter_inc.elapsed_time_millis() / 1000.0)
                      << ", t_sketch_inc=" << (stats_.t_sketch_inc.elapsed_time_millis() / 1000.0)
                      << ", t_swaps=" << (stats_.t_swaps.elapsed_time_millis() / 1000.0);

            if constexpr(sketch_batching_) {
                std::cout << ", t_sketch_batches=" << (stats_.t_sketch_batch.elapsed_time_millis() / 1000.0);
            }
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
