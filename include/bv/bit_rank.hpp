#pragma once

#include <bit>
#include <limits>
#include <memory>
#include <utility>

#include <word_packing.hpp>

#include "../idiv_ceil.hpp"

/// \brief A space efficient data structure for answering rank queries on a \ref BitVector in constant time.
///
/// A rank query counts the number of set or unset bits, respectively, from the beginning up to a given position.
/// The data structure uses a hierarchical scheme dividing the bit vector into \em blocks (of 64 bits) and
/// \em superblocks and precomputes a number of rank queries, storing them in a space efficient manner.
/// On the lowest level, it uses \c popcnt instructions.
///
/// The size of a superblock is configurable via the template parameter.
/// The default value of 12 yields a very good trade-off between time and space.
/// However, query times can be reduced by about 15% by setting it to \c 16, which, however, will increase space usage by about 25%.
///
/// Note that this data structure is \em static.
/// It maintains a pointer to the underlying bit vector and will become invalid if that bit vector is changed after construction.
///
/// \tparam t_supblock_bit_width the bit width of superblock entries, a superblock will contain <tt>2^t_supblock_bit_width</tt> bits
template<std::unsigned_integral Pack, uint64_t supblock_bit_width_ = 12>
class BitRank {
private:
    template<std::unsigned_integral T>
    static size_t popcount_ls(T v, size_t const x) {
        return std::popcount(v & (std::numeric_limits<T>::max() >> (std::numeric_limits<T>::digits  - 1 - x)));
    }

    static constexpr size_t SUP_W = supblock_bit_width_;
    static constexpr size_t SUP_SZ = 1ULL << SUP_W;

    static constexpr size_t BLOCK_SZ = 8 * sizeof(Pack);
    static_assert(BLOCK_SZ <= 64);
    static constexpr size_t BLOCKS_PER_SB = SUP_SZ / BLOCK_SZ;

    Pack const* bits_;

    std::unique_ptr<Pack[]> blocks_;      // size 64 each
    std::unique_ptr<size_t[]> supblocks_; // size SUP_SZ each

public:
    /// \brief Constructs the rank data structure for the given bit vector.
    /// \param bv the bit vector
    BitRank(Pack const* bits, size_t const n) : bits_(bits) {
        size_t const num_blocks = idiv_ceil(n, BLOCK_SZ);
        blocks_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(num_blocks, SUP_W));
        supblocks_ = std::make_unique<size_t[]>(idiv_ceil(n, SUP_SZ));

        auto blocks = word_packing::accessor(blocks_.get(), SUP_W);

        // construct
        {
            size_t rank_bv = 0; // 1-bits in whole BV
            size_t rank_sb = 0; // 1-bits in current superblock
            size_t cur_sb = 0;  // current superblock

            for(size_t j = 0; j < num_blocks; j++) {
                if(j % BLOCKS_PER_SB == 0) {
                    // we reached a new superblock
                    supblocks_[cur_sb++] = rank_bv;
                    rank_sb = 0;
                }
                
                blocks[j] = rank_sb;

                const auto rank_b = std::popcount(bits[j]);
                rank_sb += rank_b;
                rank_bv += rank_b;
            }
        }
    }

    /// \brief Constructs an empty, uninitialized rank data structure.
    inline BitRank() {
    }

    BitRank(BitRank&& other) = default;
    BitRank& operator=(BitRank&& other) = default;

    BitRank(const BitRank& other) = delete;
    BitRank& operator=(const BitRank& other) = delete;

    /// \brief Counts the number of set bit (1-bits) from the beginning of the bit vector up to (and including) position \c x.
    /// \param x the position until which to count
    size_t rank1(const size_t x) const {
        auto blocks = word_packing::accessor(blocks_.get(), SUP_W);

        const size_t r_sb = supblocks_[x / SUP_SZ];
        const size_t j   = x / BLOCK_SZ;
        const size_t r_b = blocks[j];
        
        return r_sb + r_b + popcount_ls(bits_[j], x % BLOCK_SZ);
    }

    /// \brief Counts the number of set bits from the beginning of the bit vector up to (and including) position \c x.
    ///
    /// This is a convenience alias for \ref rank1.
    ///
    /// \param x the position until which to count
    inline size_t operator()(size_t x) const {
        return rank1(x);
    }

    /// \brief Counts the number of unset bits (0-bits) from the beginning of the bit vector up to (and including) position \c x.
    /// \param x the position until which to count
    inline size_t rank0(size_t x) const {
        return x + 1 - rank1(x);
    }
};
