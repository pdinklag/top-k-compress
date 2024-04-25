#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include <idiv_ceil.hpp>

#include <iopp/util/bit_unpacker.hpp>
#include <iopp/util/bit_packer.hpp>

#include <word_packing.hpp>

#include <code/huffman.hpp>

#include "rrr_classes.hpp"

struct RRRBase {
    static constexpr size_t BLOCK_BITS = 24;
    using Block = uint32_t;
    static_assert(BLOCK_BITS < std::numeric_limits<Block>::digits);

    static constexpr size_t BLOCK_CLASS_BITS = std::bit_width(BLOCK_BITS);
    static_assert(BLOCK_CLASS_BITS <= 8);
    using BlockClass = uint8_t;
    using BlockOffset = Block;

    using Classes = RRRClasses<BLOCK_BITS, Block, BlockClass>;
    static Classes C;
};

RRRBase::Classes RRRBase::C;

template<bool with_rank_>
class RRR : public RRRBase {
private:
    using Pack = iopp::PackWord;

    static constexpr size_t BLOCKS_PER_SUPERBLOCK = 170;
    static constexpr size_t SUPERBLOCK_BITS = BLOCK_BITS * BLOCKS_PER_SUPERBLOCK;

    static constexpr size_t block_rank_bits_ = std::bit_width(SUPERBLOCK_BITS);

    size_t num_blocks_;
    size_t num_superblocks_;

    std::unique_ptr<Pack[]> block_ranks_;
    size_t superblock_rank_bits_;
    std::unique_ptr<Pack[]> superblock_ranks_;

    using BlockClassHuffmanTree = code::HuffmanTree<BlockClass>;
    BlockClassHuffmanTree huff_block_classes_;
    std::vector<Pack> block_data_;

public:
    RRR() {
    }

    RRR(RRR&& other) = default;
    RRR& operator=(RRR&& other) = default;

    RRR(RRR const& other) = delete;
    RRR& operator=(RRR const& other) = delete;

    RRR(Pack const* bits, size_t const n) {
        num_blocks_ = idiv_ceil(n, BLOCK_BITS);
        num_superblocks_ = idiv_ceil(n, SUPERBLOCK_BITS);

        iopp::BitUnpacker in(bits, bits + idiv_ceil(n, iopp::PACK_WORD_BITS));

        // compute block ranks, classes and offsets
        size_t rank_total = 0;
        if constexpr(with_rank_) block_ranks_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(num_blocks_, block_rank_bits_));
        auto block_ranks = word_packing::accessor<block_rank_bits_>(block_ranks_.get());

        {
            auto block_classes = std::make_unique<BlockClass[]>(num_blocks_); // to be compressed later
            auto block_offsets = std::make_unique<BlockOffset[]>(num_blocks_); // to be compressed later

            // count bits and determine class/offset for each block
            size_t cur_block = 0;
            for(size_t i = 0; i < n; i += BLOCK_BITS) {
                Block block = 0;
                size_t rank_block = 0;
                for(size_t j = 0; j < BLOCK_BITS && i + j < n; j++) {
                    Block const b = in.read() ? 1 : 0;
                    if(b) ++rank_block;
                    block = (block << 1) | b;
                }

                if constexpr(with_rank_) block_ranks[cur_block] = rank_block;
                block_classes[cur_block] = C.block_class[block];
                block_offsets[cur_block] = C.block_offs[block];

                rank_total += rank_block;
                ++cur_block;
            }

            // compress block classes and offsets
            huff_block_classes_ = BlockClassHuffmanTree(block_classes.get(), block_classes.get() + num_blocks_);
            {
                auto out = iopp::BitPacker(std::back_insert_iterator(block_data_), false);

                auto data_offsets = std::make_unique<size_t[]>(num_blocks_);
                for(size_t i = 0; i < num_blocks_; i++) {
                    data_offsets[i] = out.num_bits_written();
                    code::Huffman::encode(out, block_classes[i], huff_block_classes_);
                    out.write(block_offsets[i], C.choice_bits[block_classes[i]]);
                }

                // TODO: compress and store data_offsets
            }
            block_data_.shrink_to_fit();
        }

        // compute superblock ranks
        if constexpr(with_rank_) {
            superblock_rank_bits_ = std::bit_width(rank_total);
            superblock_ranks_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(num_superblocks_, superblock_rank_bits_));
            auto superblock_ranks = word_packing::accessor(superblock_ranks_.get(), superblock_rank_bits_);

            {
                size_t cur_superblock = 0;
                for(size_t i = 0; i < num_blocks_; i+= BLOCKS_PER_SUPERBLOCK) {
                    size_t rank_superblock = 0;
                    for(size_t j = 0; j < BLOCKS_PER_SUPERBLOCK && i + j < num_blocks_; j++) {
                        rank_superblock += block_ranks[i + j];
                    }

                    superblock_ranks[cur_superblock] = rank_superblock;
                    ++cur_superblock;
                }
            }
        }
    }

    size_t rank1(size_t const i) const {
        if constexpr(with_rank_) {
            return 0; // TODO: implement
        } else {
            std::abort();
            return 0;
        }
    }

    bool operator[](size_t const i) const {
        return 0; // TODO: implement
    }

    size_t alloc_size() const {
        return 
            (with_rank_ ? word_packing::num_packs_required<Pack>(num_blocks_, block_rank_bits_) * sizeof(Pack) : 0) +           // block_ranks_
            (with_rank_ ? word_packing::num_packs_required<Pack>(num_superblocks_, superblock_rank_bits_) * sizeof(Pack) : 0) + // superblock_ranks_
            block_data_.capacity() * sizeof(Pack) +                                                       // block_data_
            huff_block_classes_.size() * sizeof(BlockClassHuffmanTree::Node);                             // huff_block_classes_
    }
};
