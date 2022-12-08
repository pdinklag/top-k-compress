#pragma once

#include <array>
#include <bit>
#include <concepts>
#include <cstddef>
#include <iterator>
#include <limits>
#include <type_traits>

#include <word_packing.hpp>

#include <tdc/util/concepts.hpp>

#include "binary_rank.hpp"

template <std::integral Char>
class WaveletTree {
private:
    using UChar = std::make_unsigned_t<Char>;
    static_assert(std::numeric_limits<UChar>::digits <= 16, "only alphabets up to 16 bits are supported");
    static constexpr size_t sigma_max_ = std::numeric_limits<UChar>::max() + 1;

    struct EffectiveAlphabetEntry {
        UChar  mapped;
        size_t occ;
    } __attribute__((packed));

    using Block = uintmax_t;
    using Level = BinaryRank<Block>;

    size_t n_;
    std::array<EffectiveAlphabetEntry, sigma_max_> ea_;

    size_t num_levels_;
    std::unique_ptr<Level[]> levels_;

public:
    template<tdc::InputIterator<Char> It>
    inline WaveletTree(It begin, It const end) {
        It const rewind = begin;
        size_t sigma = 0;

        // compute histogram and effective alphabet
        {
            // count number of occurrences for each character
            n_ = 0;
            ea_ = {{0, 0}};
            while(begin != end) {
                ++ea_[UChar(*begin++)].occ;
                ++n_;
            }

            // compute effective alphabet
            ea_ = {{0, 0}};
            for (size_t c = 0; c < sigma_max_; c++)
            {
                if (ea_[c].occ > 0) ea_[c].mapped = UChar(sigma++);
            }
        }

        // allocate bit vectors
        num_levels_ = std::bit_width(sigma - 1);
        auto bit_vectors = std::make_unique<std::unique_ptr<Block[]>[]>(num_levels_);

        // construct bottom-up
        {
            // initialize histogram and borders
            auto const sigma2 = 1ULL << num_levels_;
            auto h = std::make_unique<size_t[]>(sigma2);
            auto borders = std::make_unique<size_t[]>(sigma2);
            {
                size_t s = 0;
                for (size_t c = 0; c < sigma_max_; c++)
                {
                    if(ea_[c].occ > 0) h[s++] = ea_[c].occ;
                }
                assert(s == sigma);
            }

            for (size_t j = 0; j < num_levels_; j++)
            {
                auto const l = num_levels_ - 1 - j;
                auto const num_level_nodes = (1ULL << l) - 1;

                // update borders
                for (size_t v = 0; v < num_level_nodes; v++)
                {
                    h[v] = h[2 * v] + h[2 * v + 1];
                }
                borders[0] = 0;
                for (size_t v = 1; v < num_level_nodes; v++)
                {
                    borders[v] = borders[v - 1] + h[v - 1];
                }

                // build level
                bit_vectors[l] = std::make_unique<Block[]>(word_packing::num_packs_required<Block>(n_, 1));
                auto bits = word_packing::bit_accessor(bit_vectors[l].get());
                auto const rsh = num_levels_ - l;
                begin = rewind;
                while(begin != end) {
                    auto const c = ea_[UChar(*begin++)].mapped;
                    auto const v = c >> rsh;
                    auto const pos = borders[v]++;
                    bits[pos] = (v & 1);
                }
            }
        }

        // build level rank data structures
        levels_ = std::make_unique<Level[]>(num_levels_);
        for (size_t l = 0; l < num_levels_; l++)
        {
            levels_[l] = Level(std::move(bit_vectors[l]));
        }
    }

    inline size_t rank(Char const c, size_t i) const {
        if(ea_[UChar(c)].occ > 0) {
            auto const x = ea_[UChar(c)].mapped;
            size_t a = 0;
            size_t b = n_;

            for (size_t l = 0; l < num_levels_; l++)
            {
                auto const &level = levels_[l];
                size_t const left_child_size = b ? level.rank0(a, b - 1) : 0;

                bool const bit = (x >> (num_levels_ - 1 - l)) & 1;
                if (bit)
                {
                    // 1-bit, navigate right
                    i = level.rank1(a, a + i);
                    a = a + left_child_size;
                }
                else
                {
                    // 0-bit, navigate left
                    i = level.rank0(a, a + i);
                    b = a + left_child_size;
                }
            }
            return i + 1 - a;
        } else {
            return 0;
        }
    }

    inline size_t length() const { return n_; }

    inline size_t occ(Char const c) const { return ea_[UChar(c)].occ; }
};
