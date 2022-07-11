#include <cstddef>
#include <cstdint>
#include <memory>

#include <tdc/util/concepts.hpp>

#include "count_min.hpp"
#include "rolling_karp_rabin.hpp"

class TopKSubstrings {
private:
    static constexpr size_t hash_window_size_ = 8;

    static constexpr uint64_t rolling_fp_offset_ = (1ULL << 63) - 25;
    static constexpr uint64_t rolling_fp_base_ = (1ULL << 14) - 15;

    RollingKarpRabin hash_;
    std::unique_ptr<char[]> hash_window_;
    CountMin<size_t> sketch_;

    size_t k_;
    size_t len_;

    size_t debug_total_;

public:
    inline TopKSubstrings(size_t const k, size_t const len, size_t sketch_rows, size_t sketch_columns)
    : hash_(hash_window_size_, rolling_fp_base_),
      hash_window_(std::make_unique<char[]>(len + 1)), // +1 is just for debugging purposes...
      sketch_(sketch_rows, sketch_columns),
      k_(k),
      len_(len),
      debug_total_(0) {
    }
    
    struct LongestFrequentPrefix {
        size_t index;
        size_t length;
    };

    template<typename Str>
    LongestFrequentPrefix count_prefixes(Str const& s) {
        // init
        size_t i = 0;
        LongestFrequentPrefix match = { SIZE_MAX, 0 };

        uint64_t fp = rolling_fp_offset_;

        for(size_t i = 0; i < len_; i++) {
            // get next character
            ++debug_total_;
            char const c = s[i];

            // update fingerprint
            {
                hash_window_[i] = c;
                char pop = (i >= hash_window_size_ ? hash_window_[i - hash_window_size_] : 0);
                fp = hash_.roll(fp, pop, c);
            }

            // TODO: process prefix of length i+1

            // count in sketch
            // TODO: only do it if not frequent!
            {
                // debug print current prefix and fingerprint
                {
                    // hash_window_[i + 1] = 0;
                    // std::cout << hash_window_.get() << " -> fp=0x" << std::hex << fp << std::dec << std::endl;
                }

                auto est = sketch_.increment_and_estimate(fp);
                // TODO: test if now frequent!
            }
        }
        return match;
    }

    void print_debug_info() const {
        std::cout << "debug_total=" << debug_total_ << std::endl;
        sketch_.print_debug_info();
    }
};
