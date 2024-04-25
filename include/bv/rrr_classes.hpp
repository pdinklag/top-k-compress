#pragma once

#include <array>
#include <concepts>
#include <cstddef>

template<size_t block_bits_, std::unsigned_integral Block, std::unsigned_integral BlockClass>
struct RRRClasses {
private:
    using BlockOffset = Block;

    static constexpr size_t NUM_DISTINCT_BLOCKS = 1ULL << block_bits_;
    static constexpr Block BLOCK_MAX = NUM_DISTINCT_BLOCKS - 1;

    static constexpr size_t fact(size_t const n) {
        size_t f = 1;
        for(size_t i = 2; i <= n; i++) {
            f *= i;
        }
        return f;
    }

    static constexpr size_t num_choices(size_t const n, size_t const k) {
        return fact(n) / (fact(k) * fact(n-k));
    }

    static constexpr Block next_permutation(Block const v) {
        // source: https://graphics.stanford.edu/~seander/bithacks.html#NextBitPermutation
        Block const t = (v | (v - 1)) + 1;  
        return t | ((((t & -t) / (v & -v)) >> 1) - 1);
    }

public:
    std::array<Block, block_bits_ + 1> choice_num;
    std::array<Block, block_bits_ + 1> choice_bits;
    std::array<Block, block_bits_ + 1> choice_offs;

    std::array<Block, NUM_DISTINCT_BLOCKS> blocks;

    std::array<BlockClass, NUM_DISTINCT_BLOCKS> block_class;
    std::array<BlockOffset, NUM_DISTINCT_BLOCKS> block_offs;

    constexpr RRRClasses() {
        size_t offs = 0;
        for(size_t k = 0; k <= block_bits_; k++) {
            size_t const num_blocks = num_choices(block_bits_, k);
            choice_num[k] = num_blocks;
            choice_bits[k] = std::bit_width(num_blocks - 1);
            choice_offs[k] = offs;

            // initialize distinct permutations of blocks where exactly k bits are set
            if(k == 0) {
                block_class[0] = 0;
                block_offs[0] = 0;
                blocks[offs++] = 0;
            } else {
                Block block = BLOCK_MAX >> (block_bits_ - k); // initial permutation
                for(size_t i = 0; i < num_blocks; i++) {
                    block_class[block] = k;
                    block_offs[block] = i;
                    blocks[offs++] = block;
                    block = next_permutation(block);
                }
            }
        }
    }
};
