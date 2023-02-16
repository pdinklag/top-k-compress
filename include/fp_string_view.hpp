#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include <tlx/container/ring_buffer.hpp>

#include <rolling_karp_rabin.hpp>

// represents a string view enhanced by Karp-Rabin fingerprints
template<std::integral Char>
class FPStringView {
private:
    using UChar = std::make_unsigned_t<Char>;

    static constexpr size_t fp_window_ = 512;
    static constexpr size_t fp_base_ = 512 - 9;

    std::string_view view_;
    std::shared_ptr<std::vector<uint64_t>> fp_;

    tlx::RingBuffer<UChar> buffer_;
    RollingKarpRabin rolling_hash_;

    void ensure_buffer() {
        if(buffer_.max_size() == 0) {
            buffer_ = tlx::RingBuffer<UChar>(fp_window_);
        }
    }

public:
    FPStringView(std::string_view const& s)
        : view_(s),
          fp_(std::make_shared<std::vector<uint64_t>>(s.length())),
          buffer_(fp_window_),
          rolling_hash_(fp_window_, fp_base_) {
        
        (*fp_)[0] = rolling_hash_.roll(0, 0, s[0]);
        for(size_t i = 1; i < s.length(); i++) {
            bool const roll = (buffer_.size() == buffer_.max_size());
            UChar const c = UChar(s[i]);
            UChar const pop_left = roll ? buffer_.front() : 0;

            (*fp_)[i] = rolling_hash_.roll((*fp_)[i-1], pop_left, c);

            if(roll) buffer_.pop_front();
            buffer_.push_back(c);
        }
    }

    FPStringView(FPStringView const& s, size_t const len)
        : view_(s.view_.begin(), s.view_.at(len)),
          fp_(s.fp_),
          buffer_(),
          rolling_hash_(s.rolling_hash_)
    {
        assert(len < s.length());
    }

    std::string_view string_view() const { return view_; }

    // returns the i-th character
    char operator[](size_t const i) const { return view_[i]; }

    // returns the length of the string
    size_t length() const { return view_.length(); }

    // returns the fingerprint of the string from its beginning up to position (including) i
    uint64_t const fingerprint(size_t const i) const { return (*fp_)[i]; }
};
