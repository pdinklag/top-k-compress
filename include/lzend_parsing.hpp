#pragma once

#include <cassert>
#include <concepts>
#include <iterator>
#include <vector>

#include <display.hpp>
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
    } __attribute__((packed));

private:
    struct PhraseEnd {
        Index pos;
        Index phr;

        // std::totally_ordered
        bool operator==(PhraseEnd const& x) const { return pos == x.pos; }
        bool operator!=(PhraseEnd const& x) const { return pos != x.pos; }
        bool operator< (PhraseEnd const& x) const { return pos <  x.pos; }
        bool operator<=(PhraseEnd const& x) const { return pos <= x.pos; }
        bool operator> (PhraseEnd const& x) const { return pos >  x.pos; }
        bool operator>=(PhraseEnd const& x) const { return pos >= x.pos; }
    } __attribute__((packed));

    Index text_len_;
    std::vector<Phrase> phrases_;
    BTree<PhraseEnd, 65> ends_;

public:
    LZEndParsing() : text_len_(0) {
        phrases_.emplace_back(0, 0, -1, 0);
    }

    // accesses the i-th phrase (1-based, read-only)
    Phrase const& operator[](Index const i) const { return phrases_[i]; }

    // appends a new LZ-End phrase
    void emplace_back(Index const link, Index const len, Char const last) {
        assert(len);

        auto const p = (Index)phrases_.size();
        text_len_ += len;
        phrases_.emplace_back(link, len, text_len_ - 1, last);
        ends_.insert({text_len_ - 1, p});
    }

    // appends a new LZ-End phrase
    void push_back(Phrase const& phrase) {
        assert(phrase.len);

        auto const p = (Index)phrases_.size();
        text_len_ += phrase.len;
        phrases_.push_back(phrase);
        ends_.insert({text_len_ - 1, p});
    }

    // pops the last LZ-End phrase
    Phrase pop_back() {
        assert(size() > 0);

        auto const last = phrases_.back();
        assert(text_len_ >= last.len);

        phrases_.pop_back();
        ends_.remove({text_len_ - 1, 0});
        text_len_ -= last.len;
        return last;
    }

    // replaces the last LZ-End phrase
    void replace_back(Index const link, Index const len, Char const last) {
        pop_back();
        emplace_back(link, len, last);
    }

    // extracts the substring of the text of given length and starting at the given position
    template<std::output_iterator<Char> Out>
    void extract(Out out, Index const start, Index const len) const {
        auto const end = start + len - 1;
        auto const r = ends_.successor({end,0});
        assert(r.exists);
        auto const p = r.key.phr;
        if(r.key.pos == end) {
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

    // gets the number of the phrase (1-based) that the given text position (0-based) lies in
    Index phrase_at(Index const text_pos) const {
        auto const r = ends_.successor({text_pos,0});
        return r.exists ? r.key.phr : 0;
    }

    // gets the length of the represented text
    Index length() const { return text_len_; }

    // gets the number of phrases
    auto size() const { return phrases_.size() - 1; }
};