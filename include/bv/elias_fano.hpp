#pragma once

#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include <word_packing.hpp>

class EliasFano {
private:
    using Pack = uintmax_t;

    std::unique_ptr<Pack[]> lower_;
    size_t lower_bits_;
    word_packing::BitVector upper_;

public:
    template<std::unsigned_integral T>
    EliasFano(T const* array, size_t const n) {
        auto const log_n = std::bit_width(n-1);

        auto const m = array[n-1];
        auto const log_m = std::bit_width(m);
        
        lower_bits_ = log_m - log_n;
        
        auto const lower_mask = (1ULL << lower_bits_) - 1;
        auto extract_lower = [&](T const v){ return v & lower_mask; };

        auto const upper_mask = (1ULL << log_n) - 1;
        auto extract_upper = [&](T const v) { return (v >> lower_bits_) & upper_mask; };

        auto lower = word_packing::alloc(lower_, n, lower_bits_);

        auto const max_block = upper_mask;
        size_t cur_block = 0;
        for(size_t i = 0; i < n; i++) {
            auto const v = array[i];
            assert(i == 0 || v > array[i-1]);
            lower[i] = extract_lower(v);

            auto const block = extract_upper(v);
            while(block > cur_block) {
                upper_.push_back(0);
                ++cur_block;
            }
            upper_.push_back(1);
        }

        while(cur_block <= max_block) {
            upper_.push_back(0);
            ++cur_block;
        }
    }
};
