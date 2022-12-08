#pragma once

#include <array>
#include <concepts>
#include <type_traits>
#include <utility>

#include <tdc/util/concepts.hpp>

#include "wavelet_tree.hpp"

template <std::integral Char>
class BackwardSearch {
private:
    using UChar = std::make_unsigned_t<Char>;
    static constexpr size_t sigma_max_ = std::numeric_limits<UChar>::max() + 1;

    WaveletTree<Char> wt_;
    std::array<size_t, sigma_max_> c_array_;

public:
    using Interval = std::pair<size_t, size_t>;

    template<tdc::InputIterator<Char> It>
    inline BackwardSearch(It begin, It const end) : wt_(begin, end) {
        c_array_[0] = 0;
        for(size_t c = 1; c < sigma_max_; c++) {
            c_array_[c] = c_array_[c-1] + wt_.occ(Char(c));
        }
    }

    inline Interval init(Char const c) const {
        auto const x = UChar(c);
        return { c_array_[c], c < sigma_max_ ? c_array_[c+1] : wt_.length(); }
    }

    inline Interval step(Interval const x, Char const c) const {
        auto const off = c_array_[c];
        return {
            off + (start > 0 ? wt_.rank(c, start-1) : 0),
            off + (end > 0 ? wt_.rank(c, end-1) : 0)
        };
    }
};