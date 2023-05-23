#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include <tlx/container/ring_buffer.hpp>

#include <mersenne61.hpp>

// represents a string view enhanced by Karp-Rabin fingerprints
template<std::integral Char>
class FPStringView {
private:
    using UChar = std::make_unsigned_t<Char>;

    static constexpr size_t BASE = 256;

    std::string_view view_;
    std::vector<uint64_t> fp_;
    std::vector<uint64_t> pow_base_;

public:
    FPStringView(std::string_view const& s) : view_(s), fp_(s.length()), pow_base_(s.length()) { 
        fp_[0] = s[0];
        pow_base_[0] = 1;

        for(size_t i = 1; i < s.length(); i++) {
            UChar const c = UChar(s[i]);

            fp_[i] = Mersenne61::mod(uint128_t(fp_[i-1]) * uint128_t(BASE) + uint128_t(c));
            pow_base_[i] = Mersenne61::mulmod(pow_base_[i-1], BASE);
        }
    }

    std::string_view string_view() const { return view_; }

    // returns the i-th character
    char operator[](size_t const i) const { return view_[i]; }

    // returns the length of the string
    size_t length() const { return view_.length(); }

    // returns the fingerprint of the string from its beginning up to position (including) i
    uint64_t const fingerprint(size_t const i) const { return fp_[i]; }

    // returns the fingerprint of the substring starting at i and ending at j (both included)
    uint64_t const fingerprint(size_t const i, size_t const j) const {
        assert(i <= j);
        if(i) {
            auto const fp_j = fingerprint(j);
            auto const fp_i_shifted = Mersenne61::mulmod(pow_base_[j-i+1], fingerprint(i-1));
            if (fp_j >= fp_i_shifted) {
                return fp_j - fp_i_shifted;
            } else {
                return Mersenne61::PRIME - (fp_i_shifted - fp_j);
            }
        } else {
            return fingerprint(j);
        }
    }
};
