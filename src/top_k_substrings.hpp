#include <cstddef>
#include <memory>

#include <tdc/util/concepts.hpp>

#include "rolling_karp_rabin.hpp"

class TopKSubstrings {
private:
    static constexpr size_t hash_window_size_ = 8;

    RollingKarpRabin hash_;
    std::unique_ptr<char[]> hash_window_;

    size_t k_;
    size_t len_;

public:
    inline TopKSubstrings(size_t const k, size_t const len)
    : hash_(hash_window_size_, 256),
      hash_window_(std::make_unique<char[]>(len)),
      k_(k),
      len_(len) {
    
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

        uint64_t fp = 0;

        for(size_t i = 0; i < len_; i++) {
            // get next character
            char const c = s[i];

            // update fingerprint
            {
                hash_window_[i] = c;
                char pop = (i >= hash_window_size_ ? hash_window_[i - hash_window_size_] : 0);
                fp = hash_.roll(fp, pop, c);
            }

            // TODO: process prefix of length i+1
        }
        return match;
    }
};
