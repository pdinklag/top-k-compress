#pragma once

#include <cassert>
#include <concepts>
#include <iterator>
#include <vector>

#include <display.hpp>
#include <index/binary_search_hybrid.hpp>
#include <index/btree.hpp>

// represents a dynamically growing LZ-End parsing
template<std::integral Char, std::unsigned_integral Index>
class LZEndParsing {
public:
    struct Phrase {
        Index link;
        Index len;
        Index end;
        Char last;

        // comparators for successor search
        inline bool operator< (Index x) const { return end <  x; }
        inline bool operator<=(Index x) const { return end <= x; }
        inline bool operator>=(Index x) const { return end >= x; }
        inline bool operator> (Index x) const { return end >  x; }
    } __attribute__((packed));

    using Predicate = std::function<bool(Char)>;

private:
    static constexpr size_t succ_sampling_ = (1ULL << 8);

    Index text_len_;
    std::vector<Phrase> phrases_;

    KeyValueResult<Index, Index> successor(Index const pos) const {
        assert(phrases_.size() > 1);

        // we reduce the search range by excluding the first (empty) phrase
        auto r = BinarySearchHybrid<Phrase>::successor(phrases_.data() + 1, phrases_.size() - 1, pos);
        if(r.exists) {
            auto const i = r.pos + 1; // account for smaller search range
            return { true, phrases_[i].end, (Index)i };
        } else {
            return KeyValueResult<Index, Index>::none();
        }
    }

public:
    LZEndParsing() : text_len_(0) {
        phrases_.emplace_back(0, 0, -1, 0);
    }

    LZEndParsing(LZEndParsing&&) = default;
    LZEndParsing& operator=(LZEndParsing&&) = default;

    // accesses the i-th phrase (1-based, read-only)
    Phrase const& operator[](Index const i) const { return phrases_[i]; }

    // appends a new LZ-End phrase
    // if persist is false, the successor data structure will not be updated
    void emplace_back(Index const link, Index const len, Char const last) {
        assert(len);

        auto const p = (Index)phrases_.size();
        text_len_ += len;
        phrases_.emplace_back(link, len, text_len_ - 1, last);
        if(p > 1) assert(phrases_[p].end > phrases_[p-1].end);
    }

    // pops the last LZ-End phrase
    // if persist is false, the successor data structure will not be updated
    Phrase pop_back() {
        assert(size() > 0);

        auto const last = phrases_.back();
        assert(text_len_ >= last.len);

        phrases_.pop_back();
        text_len_ -= last.len;
        return last;
    }

    // replaces the last LZ-End phrase
    // if persist is false, the successor data structure will not be updated
    void replace_back(Index const link, Index const len, Char const last) {
        pop_back();
        emplace_back(link, len, last);
    }

    // decodes a suffix of the original text in reverse, beginning at the end of the given phrase number
    void decode_rev(Predicate predicate, Index p, Index num) const {
        // LIFO queue
        static std::vector<std::pair<Index, Index>> queue;
        queue.clear();
        queue.emplace_back(p, num);

        while(!queue.empty()) {
            std::tie(p, num) = queue.back();
            queue.pop_back();
            assert(p);
            assert(num);

            auto const plen = phrases_[p].len;
            assert(plen > 0);
            if(num > plen) {
                // queue up the part that ends right before the p-th phrase
                auto const remain = num - plen;
                queue.emplace_back(p-1, remain);
                num -= remain;
            }

            // extract the next character
            if(!predicate(phrases_[p].last)) return;

            // continue by following links
            if(num > 1) {
                queue.emplace_back(phrases_[p].link, num-1);
            }
        }
    }

    template<std::output_iterator<Char> Out>
    void decode_rev(Out out, Index const p, Index const num) const {
        decode_rev([&](Char const c){
            *out++ = c;
            return true;
        }, p, num);
    }

    // gets the number of the phrase (1-based) that the given text position (0-based) lies in
    Index phrase_at(Index const text_pos) const {
        auto const r = successor(text_pos);
        return r.exists ? r.value : 0;
    }

    // gets the length of the represented text
    Index length() const { return text_len_; }

    // gets the number of phrases
    auto size() const { return phrases_.size() - 1; }

    size_t memory_size() const {
        return phrases_.capacity() * sizeof(Phrase);
    }
};
