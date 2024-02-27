#pragma once

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "space_saving.hpp"

template<std::unsigned_integral StringIndex = uint32_t>
class KAttractor {
private:
    class PosData {
    public:
        using Character = char;
        using Index = StringIndex;

    private:
        static constexpr Index NIL = -1;

        Index freq_;  // the current frequency
        Index prev_;  // the previous node in frequency order
        Index next_;  // the next node in frequency order

    public:
        PosData() : freq_(0), prev_(NIL), next_(NIL) {
        }

        // SpaceSavingItem
        Index freq() const ALWAYS_INLINE { return freq_; }
        Index prev() const ALWAYS_INLINE { return prev_; }
        Index next() const ALWAYS_INLINE { return next_; }
        
        bool is_linked() const ALWAYS_INLINE { return true; }

        void freq(Index const f) ALWAYS_INLINE { freq_ = f; }
        void prev(Index const x) ALWAYS_INLINE { prev_ = x; }
        void next(Index const x) ALWAYS_INLINE { next_ = x; }
    } __attribute__((packed));

    size_t k_;
    std::unique_ptr<char[]> attr_;
    std::unique_ptr<PosData[]> data_;
    SpaceSaving<PosData, true> space_saving_;

public:
    struct MatchResult {
        std::unique_ptr<StringIndex[]> ms;

        StringIndex pos;
        StringIndex len;
        char mismatch;

        MatchResult(size_t const k) : ms(std::make_unique<StringIndex[]>(k)) {
        }
    };

    inline KAttractor() : k_(0) {
    }

    inline KAttractor(size_t const k, size_t const max_frequency)
        : k_(k),
          attr_(std::make_unique<char[]>(k)),
          data_(std::make_unique<PosData[]>(k)),
          space_saving_(data_.get(), 0, k_ - 1, max_frequency - 1) {

        // mark all positions as garbage
        space_saving_.init_garbage(true);
    }

    void match(char const* s, size_t const n, MatchResult& r) const {
        r.len = 0;

        // compute matching statistics between attractor and input string
        for(size_t i = 0; i < k_; i++) { // FIXME: this is ultra-naive
            auto j = i;
            auto p = 0;

            StringIndex lce = 0;
            while(j < k_ && p < n && attr_[j] == s[p]) {
                ++lce;
                ++j;
                ++p;
            }

            r.ms[i] = lce;
            r.len = std::max(r.len, lce);
        }
        r.mismatch = r.len < n ? s[r.len] : 0;

        if(r.len > 1) {
            // from all possible matches, select the one with the lowest frequency at the mismatch position
            StringIndex fmin = std::numeric_limits<StringIndex>::max();

            for(size_t i = 0; i < k_; i++) {
                if(r.ms[i] == r.len && i + r.len < k_ && data_[i + r.len].freq() < fmin) {
                    r.pos = i;
                    fmin = data_[i + r.len].freq();
                }
            }
        } else {
            // otherwise (new symol), take any minimum
            r.pos = space_saving_.min();
        }
    }

    void update(MatchResult const& r) {
        for(size_t i = 0; i < r.len && r.pos + i < k_; i++) {
            space_saving_.increment(r.pos + i);
        }

        auto const m = r.pos + r.len;
        if(m < k_) {
            if(data_[m].freq() <= 1) {
                attr_[m] = r.mismatch;
                space_saving_.increment(m);
            } else {
                space_saving_.decrement(m);
            }
        }
    }

    void print_debug_info() const {
        space_saving_.print_debug_info();
        std::cout << "k-attractor:" << std::endl;
        for(size_t i = 0; i < k_; i++) {
            std::cout << display(attr_[i]) << " (" << data_[i].freq() << "), ";
        }
        std::cout << std::endl;
    }
};
