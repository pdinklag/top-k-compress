#include <cstddef>
#include <cstdint>
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

        inline Stats() : num_strings_total(0),
                         num_filter_inc(0),
                         num_sketch_inc(0),
                         num_swaps(0) {
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

        bool maybe_frequent = true;
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
            if(maybe_frequent && filter_.try_get_child(previous, c, child)) {
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
                if(!maybe_frequent || filter_.full()) {
                    // we dropped out of the filter, no extensions may be frequent
                    maybe_frequent = false;

                    // count in sketch
                    if constexpr(gather_stats_) ++stats_.num_sketch_inc;
                    auto est = sketch_.increment_and_estimate(fp);

                    // test if now frequent
                    if(est > min_pq_.min_frequency()) {
                        // it is, swap with a maximal frequent string with minimal frequency
                        if constexpr(gather_stats_) ++stats_.num_swaps;
                        // std::cout << "swap! est=" << est << ", min=" << min_pq_.min_frequency() << std::endl;
                    }

                    // invalidate previous node
                    previous = 0;

                    // std::cout << "\t-> non-frequent: " << est << std::endl;
                } else {
                    // insert into filter, which is not yet full
                    if constexpr(gather_stats_) ++stats_.num_filter_inc;

                    auto child = filter_.new_node();
                    filter_.insert_child(child, previous, c, 1);

                    // insert into min-PQ as a maximal string
                    min_pq_map_[child] = min_pq_.insert(child, 1);

                    // mark the immediate prefix of this string no longer maximal
                    min_pq_map_[previous] = min_pq_.mark_non_maximal(min_pq_map_[previous]);

                    // make new node the previous node
                    previous = child;
                }
            }
        }
        return match;
    }

    void print_debug_info() const {
        std::cout << "num_strings_total=" << stats_.num_strings_total
                  << ", num_filter_inc=" << stats_.num_filter_inc
                  << ", num_sketch_inc=" << stats_.num_sketch_inc
                  << ", num_swaps=" << stats_.num_swaps
                  << std::endl;
        // sketch_.print_debug_info();
    }
};
