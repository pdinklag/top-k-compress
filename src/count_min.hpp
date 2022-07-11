#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <random>

#include "fnv.hpp"

template<std::unsigned_integral Frequency>
class CountMin {
private:
    static constexpr size_t random_seed_ = 147;
    static constexpr uintmax_t fnv_prime_ = (1ULL << 50) - 27;
    static constexpr uintmax_t fnv_bpp_ = 13;

    using Row = std::unique_ptr<Frequency[]>;

    std::unique_ptr<Row[]> table_;
    std::unique_ptr<uintmax_t[]> hash_seed_;
    size_t num_rows_;
    size_t num_columns_;

    size_t hash(size_t const i, uintmax_t const item) const {
        return fnv(item, hash_seed_[i], fnv_prime_, fnv_bpp_) % num_columns_; // fnv may be too slow?
    }

public:
    inline CountMin(size_t const rows, size_t const columns) : num_rows_(rows), num_columns_(columns) {
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
            std::uniform_int_distribution<uintmax_t> dist(0, UINTMAX_MAX);

            hash_seed_ = std::make_unique<uintmax_t[]>(num_rows_);
            for(size_t i = 0; i < num_rows_; i++) {
                hash_seed_[i] = dist(gen);
                // std::cout << "hash_seed_[" << i << "] = 0x" << std::hex << hash_seed_[i] << std::endl;
            }
        }
    }

    inline Frequency increment_and_estimate(uintmax_t const item) {
        Frequency freq = std::numeric_limits<Frequency>::max();
        for(size_t i = 0; i < num_rows_; i++) {
            size_t const j = hash(i, item);

            ++table_[i][j];

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
};
