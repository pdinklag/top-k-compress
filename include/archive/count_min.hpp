#pragma once

#include <algorithm>
#include <bit>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>

template<std::unsigned_integral Frequency>
class CountMin {
private:
    static constexpr size_t random_seed_ = 147;
    static constexpr uintmax_t random_primes_[] = {
        (1ULL << 45) - 229,
        (1ULL << 45) - 193,
        (1ULL << 45) - 159,
        (1ULL << 45) - 139,
        (1ULL << 45) - 133,
        (1ULL << 45) - 121,
        (1ULL << 45) - 93,
        (1ULL << 45) - 81,
    };

    using Row = std::unique_ptr<Frequency[]>;

    std::unique_ptr<Row[]> table_;
    std::unique_ptr<uintmax_t[]> hash_;
    size_t num_rows_;
    size_t num_columns_;
    size_t cmask_;

    size_t hash(size_t const i, uintmax_t const item) const {
        return ((item ^ hash_[i]) % random_primes_[i & 0b111]) & cmask_;
    }

public:
    inline CountMin() : num_rows_(0), num_columns_(0) {
    }

    inline CountMin(size_t const rows, size_t const columns) {
        num_rows_ = rows;

        auto const cbits = std::bit_width(columns - 1);
        num_columns_ = 1ULL << cbits;
        cmask_ = num_columns_ - 1;

        // initialize frequency table
        table_ = std::make_unique<Row[]>(num_rows_);
        for(size_t i = 0; i < num_rows_; i++) {
            table_[i] = std::make_unique<Frequency[]>(num_columns_);
            for(size_t j = 0; j < num_columns_; j++) {
                table_[i][j] = 0;
            }
        }

        // initialize hash functions
        {
            std::mt19937_64 gen(random_seed_);

            hash_ = std::make_unique<uintmax_t[]>(num_rows_);
            for(size_t i = 0; i < num_rows_; i++) {
                hash_[i] = gen();
            }
        }
    }

    CountMin(CountMin&&) = default;
    CountMin& operator=(CountMin&&) = default;
    CountMin(CountMin const&) = delete;
    CountMin& operator=(CountMin const&) = delete;

    inline Frequency increment_and_estimate(uintmax_t const item, Frequency const inc) {
        Frequency freq = std::numeric_limits<Frequency>::max();
        for(size_t i = 0; i < num_rows_; i++) {
            size_t const j = hash(i, item);

            table_[i][j] += inc;

            // std::cout << "\th_" << i << "(fp) = " << j << " -> " << table_[i][j] << std::endl;
            freq = std::min(freq, table_[i][j]);
        }
        return freq;
    }

    inline void increment(uintmax_t const item, Frequency const inc) {
        for(size_t i = 0; i < num_rows_; i++) {
            size_t const j = hash(i, item);
            table_[i][j] += inc;
        }
    }

    struct BatchWorkItem {
        uintmax_t item;
        Frequency inc;
        Frequency est;
    };

    void print_debug_info() const {
        size_t num_zeros = 0;
        Frequency min = std::numeric_limits<Frequency>::max();
        Frequency max = 0;
        Frequency sum = 0;
        
        for(size_t i = 0; i < num_rows_; i++) {
            for(size_t j = 0; j < num_columns_; j++) {
                auto const f = table_[i][j];
                
                sum += f;
                min = std::min(min, f);
                max = std::max(max, f);
                if(f == 0) ++num_zeros;
            }
        }
        
        size_t const num_cells = num_rows_ * num_columns_;
        double const avg = ((double)sum / double(num_cells));
        double var = 0.0;
        for(size_t i = 0; i < num_rows_; i++) {
            for(size_t j = 0; j < num_columns_; j++) {
                auto const f = table_[i][j];
                double const d = (double)f - avg;
                var += d * d;
            }
        }
        var /= (double)(num_cells - 1);
        
        std::cout << "sketch info"
                  << ": bytes=" << (sizeof(Frequency) * num_cells)
                  << ", min=" << min
                  << ", max=" << max
                  << ", avg=" << ((double)sum / double(num_cells))
                  << ", stddev=" << std::sqrt(var)
                  << ", num_zeros=" << num_zeros;
        
        for(size_t i = 0; i < num_rows_; i++) {
            std::cout << ", hash[" << (i+1) << "]=0x" << std::hex << hash_[i] << std::dec;
        }
        
        std::cout << std::endl;
    }
};
