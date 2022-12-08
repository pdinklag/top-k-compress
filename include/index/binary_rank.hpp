#pragma once

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

#include <tdc/util/math.hpp>

#include <word_packing.hpp>

template <std::unsigned_integral Block = uintmax_t, size_t superblock_bit_exp_ = 12>
class BinaryRank {
private:
    static constexpr Block block_max_ = std::numeric_limits<Block>::max();
    static constexpr size_t block_bits_ = std::numeric_limits<Block>::digits;

public:
    inline static constexpr int popcnt(Block const v) {
        return std::popcount(v);
    }

    inline static constexpr int popcnt(Block const v, size_t const j) {
        Block const mask_j = block_max_ >> (~j % block_bits_); // BLOCK_BITS - 1 - j
        return popcnt(v & mask_j);
    }

    inline static constexpr int popcnt(Block const v, size_t const i, size_t const j) {
        Block const mask_i = block_max_ << i;
        Block const mask_j = block_max_ >> (~j % block_bits_); // BLOCK_BITS - 1 - j
        return popcnt(v & mask_i & mask_j);
    }

private:
    static constexpr size_t block_size_ = block_bits_;
    static constexpr size_t superblock_size_ = 1ULL << superblock_bit_exp_;

    static constexpr size_t blocks_per_superblock_ = superblock_size_ / block_size_;

    using Pack = uintmax_t;

    std::unique_ptr<Block[]> bits_;
    std::unique_ptr<Pack[]> block_ranks_;
    std::unique_ptr<uintmax_t[]> superblock_ranks_;

    inline BinaryRank(std::unique_ptr<Block[]> &&bits, size_t const n) : bits_(std::move(bits)) {
        size_t const num_blocks = tdc::idiv_ceil(n, block_size_);
        block_ranks_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(num_blocks, superblock_bit_exp_));

        size_t const num_superblocks = tdc::idiv_ceil(n, superblock_size_);
        superblock_ranks_ = std::make_unique<uintmax_t[]>(num_superblocks);

        // construct
        {
            auto block_ranks = word_packing::accessor<superblock_bit_exp_, Pack>(block_ranks_.get());

            size_t rank_bv = 0; // 1-bits in whole bit vector
            size_t rank_sb = 0; // 1-bits in current superblock
            size_t cur_sb = 0;  // number of current superblock

            for (size_t j = 0; j < num_blocks; j++)
            {
                if (j % blocks_per_superblock_ == 0)
                {
                    // we reached a new superblock
                    superblock_ranks_[cur_sb++] = rank_bv;
                    rank_sb = 0;
                }

                block_ranks[j] = rank_sb;

                auto const rank_b = popcnt(bits_[j]);
                rank_sb += rank_b;
                rank_bv += rank_b;
            }
        }
    }

    inline BinaryRank() {}
    BinaryRank(BinaryRank const& other) = default;
    BinaryRank(BinaryRank&& other) = default;
    BinaryRank& operator=(BinaryRank const& other) = default;
    BinaryRank& operator=(BinaryRank&& other) = default;

    inline size_t rank1(size_t const i) const {
        auto block_ranks = word_packing::accessor<superblock_bit_exp_, Pack>(block_ranks_.get());

        const size_t r_sb = superblock_ranks_[i / superblock_size_];
        const size_t j   = i / block_size_;
        const size_t r_b = block_ranks[j];
        
        return r_sb + r_b + popcnt(bits_[j], i % block_size_);
    }

    inline size_t rank1(size_t const i, size_t const j) const {
        if(i == 0) {
            return rank1(j);
        } else {
            return rank1(j) - rank1(i-1);
        }
    }

    inline size_t operator()(size_t const i) const { return rank1(i); }

    inline size_t rank0(size_t const i) const { return i + 1 - rank1(i); }

    inline size_t rank0(size_t const i, size_t const j) const {
        if(i == 0) {
            return rank0(j);
        } else {
            return rank0(j) - rank0(i-1);
        }
    }

    inline Block const* bits() const { return bits_.get(); }
};
