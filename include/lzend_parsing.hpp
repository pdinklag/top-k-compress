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

private:
    static constexpr size_t succ_sampling_ = (1ULL << 8);

    Index text_len_;
    std::vector<Phrase> phrases_;
    std::vector<Index> succ_index_;

    KeyValueResult<Index, Index> successor(Index const pos) const {
        assert(phrases_.size() > 1);

        // find the search range by using the sampling index
        auto const pos_s = pos / succ_sampling_;
        assert(pos_s < succ_index_.size());

        auto const a = succ_index_[pos_s];
        auto const b = (pos_s + 1 < succ_index_.size()) ? succ_index_[pos_s + 1] : phrases_.size() - 1;

        auto r = BinarySearchHybrid<Phrase>::successor(phrases_.data() + a, b - a + 1, pos);
        if(r.exists) {
            auto const i = r.pos + a; // account for smaller search range
            return { true, phrases_[i].end, (Index)i };
        } else {
            return KeyValueResult<Index, Index>::none();
        }
    }

public:
    LZEndParsing() : text_len_(0) {
        phrases_.emplace_back(0, 0, -1, 0);
    }

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

        // maintain successor index
        {
            auto const end = text_len_ - 1;
            auto const end_s = end / succ_sampling_;
            while(end_s >= succ_index_.size()) {
                succ_index_.emplace_back(p);
            }
        }
    }

    // pops the last LZ-End phrase
    // if persist is false, the successor data structure will not be updated
    Phrase pop_back() {
        assert(size() > 0);

        auto const last = phrases_.back();
        assert(text_len_ >= last.len);
        
        // maintain successor index
        {
            auto const p = phrases_.size() - 1;
            while(succ_index_.back() == p) {
                succ_index_.pop_back();
            }
        }

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

    // extracts the substring of the text of given length and starting at the given position
    template<std::output_iterator<Char> Out>
    void extract(Out out, Index const start, Index const len) const {
        auto const end = start + len - 1;
        auto const r = successor(end);
        assert(r.exists);
        auto const p = r.value;
        if(r.key == end) {
            // we're at a phrase end position
            if(len > 1) extract(out, start, len - 1);
            *out++ = phrases_[p].last;
        } else {
            // we're somewhere within phrase p and need to extract something from the source
            auto const pstart = phrases_[p-1].end + 1;
            auto const lnk_end = phrases_[phrases_[p].link].end;

            if(start < pstart) {
                // we are trying to extract some part prior to the current phrase, and that must be done separately
                extract(out, start, pstart - start);

                // extract remainder from source
                auto const suffix_len = end - pstart + 1;
                extract(out, lnk_end - suffix_len + 1, suffix_len);
            } else {
                // extract from source
                extract(out, lnk_end - len + 1, len);
            }
        }
    }

    // extracts the i-th phrase
    template<std::output_iterator<Char> Out>
    void extract_phrase(Out out, Index const i) const {
        auto const len = phrases_[i].len;
        extract(out, phrases_[i].end - len + 1, len);
    }

    // extracts the suffix of the given length of the i-th phrase
    template<std::output_iterator<Char> Out>
    void extract_phrase_suffix(Out out, Index const i, Index const len) const {
        extract(out, phrases_[i].end - len + 1, len);
    }

    // extracts the reversed substring of the text of given length and starting at the given position
    // stop if predicate returns false
    template<typename Predicate>
    bool extract_reverse_until(Predicate predicate, Index const start, Index const len) const {
        auto const end = start + len - 1;
        auto const r = successor(end);
        assert(r.exists);
        auto const p = r.value;
        if(r.key == end) {
            // we're at a phrase end position
            if(!predicate(phrases_[p].last)) return false;
            return len <= 1 || extract_reverse_until(predicate, start, len - 1);
        } else {
            // we're somewhere within phrase p and need to extract something from the source
            auto const pstart = phrases_[p-1].end + 1;
            auto const lnk_end = phrases_[phrases_[p].link].end;

            if(start < pstart) {
                // we are trying to extract some part prior to the current phrase, and that must be done separately
                // first, extract remainder from source
                auto const suffix_len = end - pstart + 1;
                if(!extract_reverse_until(predicate, lnk_end - suffix_len + 1, suffix_len)) return false;

                // now extract the part prior
                return extract_reverse_until(predicate, start, pstart - start);
            } else {
                // extract from source
                return extract_reverse_until(predicate, lnk_end - len + 1, len);
            }
        }
    }

    // extracts the reversed suffix of the given length of the i-th phrase
    template<typename Predicate>
    void extract_reverse_phrase_suffix_until(Predicate predicate, Index const i, Index const len) const {
        extract_reverse_until(predicate, phrases_[i].end - len + 1, len);
    }

    // extracts the suffix of the given length of the i-th phrase
    template<std::output_iterator<Char> Out>
    void extract_reverse_phrase_suffix(Out out, Index const i, Index const len) const {
        extract_reverse_phrase_suffix_until([&](char const c){
            *out++ = c;
            return true;
        }, i, len);
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
};
