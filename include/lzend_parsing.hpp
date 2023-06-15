#pragma once

#include <cassert>
#include <concepts>
#include <iterator>
#include <vector>

// represents a dynamically growing LZ-End parsing
template<std::integral Char, std::unsigned_integral Index>
class LZEndParsing {
public:
    struct Phrase {
        Index link;
        Index len;
        Char last;
    } __attribute__((packed));

    using Predicate = std::function<bool(Char)>;

private:
    static constexpr size_t succ_sampling_ = (1ULL << 8);

    Index text_len_;
    std::vector<Phrase> phrases_;

public:
    LZEndParsing() : text_len_(0) {
        phrases_.emplace_back(0, 0, 0);
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
        phrases_.emplace_back(link, len, last);
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

        do {
            assert(p);
            assert(num);

            while(num) {
                auto const& phrase = phrases_[p];
                assert(phrase.len > 0);
                
                // extract the next character
                if(!predicate(phrase.last)) return;
                
                if(num > phrase.len) {
                    // queue up the part that ends right before the p-th phrase
                    auto const remain = num - phrase.len;
                    queue.emplace_back(p - 1, remain);
                    num -= remain;
                }
                
                // follow link
                --num;
                if(num) p = phrase.link;
            }
            
            if(!queue.empty()) {
                std::tie(p, num) = queue.back();
                queue.pop_back();
            }
        } while(num);
    }

    // matches the reverse suffix of the original text against the given string
    // returns the number of matching characters, as well as the mismatch character
    std::pair<Index, Char> match_rev(Char const* s, Index p, Index const max) const {
        // LIFO queue
        static std::vector<std::pair<Index, Index>> queue;
        queue.clear();

        Index match = 0;
        Char mismatch;

        auto num = max + 1;
        do {
            assert(p);
            assert(num);

            while(num) {
                auto const& phrase = phrases_[p];
                assert(phrase.len > 0);
                
                // extract the next character
                mismatch = phrase.last;
                if(match < max && s[match] == mismatch) ++match;
                else return std::make_pair(match, mismatch);
                
                if(num > phrase.len) {
                    // queue up the part that ends right before the p-th phrase
                    auto const remain = num - phrase.len;
                    queue.emplace_back(p - 1, remain);
                    num -= remain;
                }
                
                // follow link
                --num;
                if(num) p = phrase.link;
            }
            
            if(!queue.empty()) {
                std::tie(p, num) = queue.back();
                queue.pop_back();
            }
        } while(num && p);

        // if we get here, it means that we have fully matched everything
        return std::make_pair(match, Char());
    }

    template<std::output_iterator<Char> Out>
    void decode_rev(Out out, Index const p, Index const num) const {
        decode_rev([&](Char const c){
            *out++ = c;
            return true;
        }, p, num);
    }

    // gets the length of the represented text
    Index length() const { return text_len_; }

    // gets the number of phrases
    auto size() const { return phrases_.size() - 1; }

    size_t memory_size() const {
        return phrases_.capacity() * sizeof(Phrase);
    }
};
