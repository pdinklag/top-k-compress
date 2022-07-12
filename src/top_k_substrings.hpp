#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>

#include <tdc/util/concepts.hpp>

#include "trie_filter.hpp"
#include "min_pq.hpp"
#include "count_min.hpp"
#include "rolling_karp_rabin.hpp"

class TopKSubstrings {
private:
    static constexpr bool gather_stats_ = true;
    struct Stats {
        size_t num_strings_total;
        size_t num_filter_inc;
        size_t num_sketch_inc;
        size_t num_swaps;
        size_t num_overestimates;

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

    RollingKarpRabin hash_;
    std::unique_ptr<char[]> hash_window_;

    TrieFilter<size_t> filter_;
    MinPQ<size_t> min_pq_;
    std::unique_ptr<MinPQ<size_t>::Location[]> min_pq_map_;
    CountMin<size_t> sketch_;

    size_t k_;
    size_t len_;

    Stats stats_;

public:
    inline TopKSubstrings(size_t const k, size_t const len, size_t sketch_rows, size_t sketch_columns)
    : hash_(hash_window_size_, rolling_fp_base_),
      hash_window_(std::make_unique<char[]>(len + 1)), // +1 is just for debugging purposes...
      filter_(k),
      min_pq_(),
      min_pq_map_(std::make_unique<MinPQ<size_t>::Location[]>(k)),
      sketch_(sketch_rows, sketch_columns),
      k_(k),
      len_(len),
      stats_() {
    }
    
    struct LongestFrequentPrefix {
        size_t index;
        size_t length;
    };

    template<typename Str>
    LongestFrequentPrefix count_prefixes(Str const& s, size_t len) {
        // init
        size_t i = 0;
        LongestFrequentPrefix match = { SIZE_MAX, 0 };

        uint64_t fp = rolling_fp_offset_;

        bool look_in_filter = true;
        size_t previous = filter_.root();

        for(size_t i = 0; i < len; i++) {
            // get next character
            if constexpr(gather_stats_) ++stats_.num_strings_total;
            char const c = s[i];

            // update fingerprint
            {
                hash_window_[i] = c;
                char pop = (i >= hash_window_size_ ? hash_window_[i - hash_window_size_] : 0);
                fp = hash_.roll(fp, pop, c);
            }

            // debug print current prefix and fingerprint
            {
                hash_window_[i + 1] = 0;
                // std::cout << "s=" << hash_window_.get() << ", fp=0x" << std::hex << fp << std::dec << std::endl;
            }

            // try and find prefix of length i+1 in filter
            size_t child;
            if(look_in_filter && filter_.try_get_child(previous, c, child)) {
                // the current prefix is frequent
                // update longest match
                match.index = child;
                match.length = i + 1;

                // increment frequency
                if constexpr(gather_stats_) ++stats_.num_filter_inc;
                auto const freq = filter_.increment(child);
                min_pq_map_[child] = min_pq_.increase_key(min_pq_map_[child]);

                // advance to next prefix
                previous = child;
            } else {
                // the current prefix is non-frequent
                if(filter_.full()) {
                    // the filter is full, count current prefix in the sketch
                    if constexpr(gather_stats_) ++stats_.num_sketch_inc;
                    auto est = sketch_.increment_and_estimate(fp);

                    // test if it is now frequent
                    if(est > min_pq_.min_frequency()) {
                        // it is now frequent, test if we can swap
                        if((previous && filter_.freq(previous) >= est) || i == 0) {
                            // std::cout << "swap! est=" << est << ", min=" << min_pq_.min_frequency() << std::endl;

                            // the immediate prefix was frequent, so yes, we can!
                            if constexpr(gather_stats_) ++stats_.num_swaps;

                            // extract maximal frequent substring with minimal frequency
                            size_t const swap = min_pq_.extract_min();

                            // extract the substring from the filter and get the fingerprint and frequency delta
                            assert(filter_.is_leaf(swap));
                            auto const extracted = filter_.extract(swap);

                            // the parent may now be maximal
                            if(extracted.parent && filter_.is_leaf(extracted.parent)) {
                                // insert into min PQ
                                min_pq_map_[extracted.parent] = min_pq_.insert(extracted.parent, filter_.freq(extracted.parent));
                                assert(min_pq_.freq(min_pq_map_[extracted.parent]) == filter_.freq(extracted.parent));
                            }

                            // count the extracted substring in the sketch as often as it had been counted in the filter
                            sketch_.increment(extracted.fingerprint, extracted.freq_delta);

                            // insert the current prefix into the filter, reusing the old entries' node ID
                            filter_.insert_child(swap, previous, c, est, fp);
                            assert(filter_.is_leaf(swap));

                            // also insert it into the min PQ
                            min_pq_map_[swap] = min_pq_.insert(swap, est);
                            assert(min_pq_.freq(min_pq_map_[swap]) == est);
                            
                            // the previous prefix is no longer maximal, remove from min PQ
                            if(previous) {
                                min_pq_map_[previous] = min_pq_.remove(min_pq_map_[previous]);
                            }

                            // make new node the previous node
                            previous = swap;
                        } else {
                            // the immediate prefix was non-frequent or its frequency was too low
                            // -> the current prefix is overestimated, abort swap
                            if constexpr(gather_stats_) ++stats_.num_overestimates;

                            // invalidate previous node
                            previous = 0;
                        }
                    } else {
                        // nope, invalidate previous node
                        previous = 0;
                    }
                } else {
                    // insert into filter, which is not yet full
                    if constexpr(gather_stats_) ++stats_.num_filter_inc;

                    auto child = filter_.new_node();
                    filter_.insert_child(child, previous, c, 1, fp);

                    // insert into min PQ as a maximal string
                    min_pq_map_[child] = min_pq_.insert(child, 1);
                    assert(min_pq_.freq(min_pq_map_[child]) == 1);

                    // mark the immediate prefix of this string no longer maximal
                    min_pq_map_[previous] = min_pq_.remove(min_pq_map_[previous]);

                    // make new node the previous node
                    previous = child;
                }

                // we dropped out of the filter, so no extension can be in the filter (even if the current prefix was inserted or swapped in)
                look_in_filter = false;
            }
        }
        return match;
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
                  << ", num_overestimates=" << stats_.num_overestimates
                  << std::endl;
        // sketch_.print_debug_info();
    }
};
