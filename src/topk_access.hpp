#include "topk_twopass_impl.hpp"

#include <bit>
#include <filesystem>
#include <iterator>

#include <iopp/file_input_stream.hpp>
#include <ordered/btree/set.hpp>
#include <ordered/range_marking/set.hpp>
#include <word_packing.hpp>

#include <bv/bit_rank.hpp>
#include <bv/rrr.hpp>
#include <idiv_ceil.hpp>
#include <small_trie.hpp>

class TopKAccess {
private:
    using Pack = uintmax_t;
    using Rank = BitRank<Pack>;

    size_t n_;
    size_t k_;
    std::unique_ptr<Pack[]> parent_;
    size_t bits_per_parent_;
    std::unique_ptr<char[]> inlabel_;
    size_t height_;
    word_packing::PackedIntVector<Pack> parsing_;
    RRR<0> literal_; // FIXME: we don't need rank on literal_
    RRR<1> start_;

    // decode
    std::unique_ptr<char[]> dec_buffer_;
    mutable size_t dec_last_; // nb: cache to avoid re-spelling of the same phrase on subsequent queries -- modified in a const function
    mutable size_t dec_last_len_;

    // stats
    size_t num_literals_;

    struct PhraseContaining {
        size_t phrase;
        size_t start;
    };

    PhraseContaining phrase_containing(size_t const i) const {
        // query what phrase contains position i of the original input
        PhraseContaining r;
        r.phrase = start_.rank1(i) - 1;
        assert(r.phrase != SIZE_MAX);

        // find the starting position of that phrase
        // nb: naive select query, but since start_ is expected to be dense, we shouldn't have to scan for long
        size_t j = i;
        while(!start_[j]) --j;
        r.start = j;

        return r;
    }

    class Iterator {
    private:
        TopKAccess const* topk_;
        size_t pos_;

        size_t phrase_;
        bool dec_literal_;
        std::shared_ptr<char[]> dec_;
        size_t dec_size_;

        union {
            char literal_value;
            size_t offs;
        } payload_;

        void advance() {
            if(pos_ + 1 < topk_->n_) {
                if(dec_literal_ || payload_.offs + 1 >= dec_size_) {
                    // we have to go to the next phrase
                    *this = Iterator(*topk_, pos_ + 1, PhraseContaining { phrase_ + 1, pos_ + 1 });
                } else {
                    // we remain within the decoded phrase, simply increment offset within the buffer
                    ++payload_.offs;
                    ++pos_;
                }
            } else {
                // we went beyond the input length
                pos_ = topk_->n_;
                dec_.reset();
            }
        }

    public:
        Iterator(TopKAccess const& topk) : topk_(&topk), pos_(topk.n_) {
        }

        Iterator(TopKAccess const& topk, size_t const i, PhraseContaining const p) : topk_(&topk), pos_(i), phrase_(p.phrase) {
            auto const v = topk_->parsing_[p.phrase];
            if(dec_literal_ = topk_->literal_[p.phrase]) {
                // literal phrase, extract the character
                payload_.literal_value = (char)v;
                dec_size_ = 0;
            } else {
                // trie phrase - spell out and extract character
                dec_ = std::make_shared<char[]>(topk_->height());
                dec_size_ = topk_->spell(v, dec_.get());
                payload_.offs = (pos_ - p.start);
            }
        }

        Iterator(TopKAccess const& topk, size_t const i) : Iterator(topk, i, topk.phrase_containing(i)) {
        }

        Iterator(Iterator const&) = default;
        Iterator& operator=(Iterator const&) = default;

        Iterator(Iterator&&) = default;
        Iterator& operator=(Iterator&&) = default;

        char operator*() const { return dec_literal_ ? payload_.literal_value : dec_[payload_.offs]; }

        bool operator==(Iterator const& other) const { return topk_ == other.topk_ && pos_ == other.pos_; }
        bool operator!=(Iterator const& other) const { return !(*this == other); }

        Iterator& operator++() {
            advance();
            return *this;
        }

        Iterator operator++(int) {
            Iterator copy(*this);
            advance();
            return copy;
        }
    };

    size_t spell_reverse(size_t const node, char* buffer) const {
        auto parent = word_packing::accessor(parent_.get(), bits_per_parent_);
        
        size_t dv = 0;
        size_t v = node;
        while(v) {
            buffer[dv++] = inlabel_[v];
            v = parent[v];
        }
        return dv;
    }

    size_t spell(size_t const node, char* buffer) const {
        auto const d = spell_reverse(node, buffer);
        std::reverse(buffer, buffer + d);
        return d;
    }

public:
    using iterator_category = std::input_iterator_tag;
    using difference_type   = std::ptrdiff_t;
    using value_type        = char;
    using pointer           = char*;
    using reference         = char&;

    TopKAccess(std::filesystem::path const& path, size_t const k)
        : n_(std::filesystem::file_size(path)),
          k_(k),
          parsing_(0, std::bit_width(k-1)) {

        // initialize phrase start bit vector
        word_packing::BitVector start;
        start.resize(n_);

        iopp::FileInputStream in(path);

        // compute trie
        auto trie = topk_twopass::compute_topk(in.begin(), in.end(), k, k >> 8);

        // compute parsing and mark phrase starts
        num_literals_ = 0;
        size_t i = 0;
        height_ = 0;

        word_packing::BitVector literal;
        in.seekg(0, std::ios::beg);
        topk_twopass::parse(in.begin(), in.end(), trie, [&](topk_twopass::Phrase f) {
            height_ = std::max(height_, f.len);

            if(f.is_literal()) {
                literal.push_back(1);
                parsing_.push_back((uint8_t)f.literal);
                ++num_literals_;
            } else {
                literal.push_back(0);
                parsing_.push_back(f.node);
            }

            start[i] = 1;
            i += f.len;
        });
        assert(i == n_);

        parsing_.shrink_to_fit();
        literal_ = RRR<0>(literal.data(), literal.size());

        // compute parent/inlabel arrays and discard trie
        bits_per_parent_ = std::bit_width(k-1);
        parent_ = std::make_unique<Pack[]>(word_packing::num_packs_required<Pack>(k, bits_per_parent_));
        auto parent = word_packing::accessor(parent_.get(), bits_per_parent_);
        inlabel_ = std::make_unique<char[]>(k);
        for(size_t v = 1; v < k; v++) {
            auto const& node = trie.node(v);
            parent[v] = node.parent;
            inlabel_[v] = node.inlabel;
        }

        trie = decltype(trie)();

        // compress start and compute rank
        start_ = RRR<1>(start.data(), n_);
        
        // initialize decode buffer
        dec_buffer_ = std::make_unique<char[]>(height_);
        dec_last_ = -1;
    }

    char operator[](size_t const i) const {
        auto const p = phrase_containing(i);
        
        auto const v = parsing_[p.phrase];
        if(literal_[p.phrase]) {
            // literal phrase, extract the character
            return (char)v;
        } else {
            // trie phrase - spell out and extract character
            if(dec_last_ != v) {
                dec_last_len_ = spell_reverse(v, dec_buffer_.get());
                dec_last_ = v;
            }
        
            auto const offs = (dec_last_len_ - 1) - (i - p.start);
            return dec_buffer_[offs];
        }
    }

    auto at(size_t const i) const { return Iterator(*this, i); }

    auto begin() const { return Iterator(*this, 0, PhraseContaining { 0, 0 }); }
    auto end() const { return Iterator(*this); }

    void extract(size_t const i, size_t const len, char* buffer) const {
        auto it = at(i);
        for(size_t j = 0; j < len; j++) {
            buffer[j++] = *it++;
        }
    }

    size_t lce(size_t const i, size_t const j) const {
        // TODO: implement LCE query?
        return 0;
    }

    size_t num_phrases() const {
        return parsing_.size();
    }

    size_t num_literals() const {
        return num_literals_;
    }

    size_t height() const {
        return height_;
    }

    size_t length() const {
        return n_;
    }

    struct AllocSize {
        size_t parent;
        size_t inlabel;
        size_t parsing;
        size_t literal;
        size_t start;
        size_t start_rank;

        AllocSize(TopKAccess const& topk) {
            parent = word_packing::num_packs_required<Pack>(topk.k_, topk.bits_per_parent_) * sizeof(Pack);
            inlabel = topk.k_ * sizeof(char);
            parsing = word_packing::num_packs_required<Pack>(topk.parsing_.capacity(), topk.parsing_.width()) * sizeof(Pack);
            literal = topk.literal_.alloc_size();
            start = topk.start_.alloc_size();
            start_rank = 0; // topk.start_rank_.alloc_size();
        }

        size_t total() const {
            return parent + inlabel + parsing + literal + start + start_rank;
        }
    };

    auto alloc_size() const {
        return AllocSize(*this);
    }
};
