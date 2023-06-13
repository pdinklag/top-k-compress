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

private:
    bool extract_fwd(Predicate predicate, Index const start, Index const len) const {
        auto const end = start + len - 1;
        auto const r = successor(end);
        assert(r.exists);
        auto const p = r.value;
        if(r.key == end) {
            // we're at a phrase end position
            if(len > 1 && !extract_fwd(predicate, start, len - 1)) return false;
            return predicate(phrases_[p].last);
        } else {
            // we're somewhere within phrase p and need to extract something from the source
            auto const pstart = phrases_[p-1].end + 1;
            auto const lnk_end = phrases_[phrases_[p].link].end;

            if(start < pstart) {
                // we are trying to extract some part prior to the current phrase, and that must be done separately
                // first, extract the part prior
                if(!extract_fwd(predicate, start, pstart - start)) return false;

                // now extract remainder from source
                auto const suffix_len = end - pstart + 1;
                return extract_fwd(predicate, lnk_end - suffix_len + 1, suffix_len);
            } else {
                // extract from source
                return extract_fwd(predicate, lnk_end - len + 1, len);
            }
        }
    }

    void extract_rev(Predicate predicate, Index start, Index len) const {
        // initialize LIFO queue of substrings to extract
        static std::vector<std::pair<Index, Index>> queue;
        queue.clear();
        queue.emplace_back(start, len);

        // work off stack
        while(!queue.empty()) {
            // get next parameter set
            std::tie(start, len) = queue.back();
            queue.pop_back();

            while(len > 0) {
                auto const end = start + len - 1;
                auto const r = successor(end);
                assert(r.exists);
                auto const p = r.value;
                if(r.key == end) {
                    // we're at a phrase end position
                    if(!predicate(phrases_[p].last)) return;
                    --len;
                } else {
                    // we're somewhere within phrase p and need to extract something from the source
                    auto const pstart = phrases_[p-1].end + 1;
                    auto const lnk_end = phrases_[phrases_[p].link].end;

                    if(start < pstart) {
                        // we are trying to extract some part prior to the current phrase, and that must be done separately
                        // queue up the part prior
                        queue.emplace_back(start, pstart - start);

                        // extract remainder from source
                        auto const suffix_len = end - pstart + 1;
                        start = lnk_end - suffix_len + 1;
                        len = suffix_len;
                    } else {
                        // extract from source
                        start = lnk_end - len + 1;
                    }
                }
            }
        }
    }

public:
    // extracts the substring of the text of given length and starting at the given position
    // this is done step by step, asking the given predicate whether or not to stop at a given character
    // the reverse flag allows extracting original substring or reversed
    template<bool reverse>
    void extract(Predicate predicate, Index const start, Index const len) const {
        if constexpr(reverse) {
            extract_rev(predicate, start, len);
        } else {
            extract_fwd(predicate, start, len);
        }
    }

    // extracts the reversed suffix of the given length of the i-th phrase
    template<bool reverse, std::output_iterator<Char> Out>
    void extract(Out out, Index const start, Index const len) const {
        extract<reverse>([&](Char const c){ *out++ = c; return true; }, start, len);
    }

    // extracts the suffix of the given length of the i-th phrase
    template<bool reverse>
    void extract_phrase_suffix(Predicate predicate, Index const i, Index const len) const {
        extract<reverse>(predicate, phrases_[i].end - len + 1, len);
    }

    // extracts the reversed suffix of the given length of the i-th phrase
    template<bool reverse, std::output_iterator<Char> Out>
    void extract_phrase_suffix(Out out, Index const i, Index const len) const {
        extract_phrase_suffix<reverse>([&](Char const c){ *out++ = c; return true; }, i, len);
    }

    // extracts the i-th phrase
    template<bool reverse>
    void extract_phrase(Predicate predicate, Index const i) const {
        extract_phrase_suffix<reverse>(predicate, i, phrases_[i].len);
    }

    // extracts the i-th phrase
    template<bool reverse, std::output_iterator<Char> Out>
    void extract_phrase(Out out, Index const i) const {
        extract_phrase_suffix<reverse>(out, i, phrases_[i].len);
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
        return phrases_.capacity() * sizeof(Phrase) + succ_index_.capacity() * sizeof(Index);
    }
};
