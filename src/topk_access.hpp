#include "topk_twopass_impl.hpp"

#include <bit>
#include <filesystem>

#include <iopp/file_input_stream.hpp>
#include <ordered/btree/set.hpp>
#include <ordered/range_marking/set.hpp>
#include <word_packing.hpp>

#include <bv/bit_rank.hpp>
#include <idiv_ceil.hpp>
#include <small_trie.hpp>

class TopKAccess {
private:
    using Pack = uintmax_t;
    using Rank = BitRank<Pack>;

    size_t n_;
    size_t k_;
    SmallTrie<true> trie_;
    size_t height_;
    word_packing::PackedIntVector<Pack> parsing_;
    word_packing::BitVector literal_;
    std::unique_ptr<Pack[]> start_;
    Rank start_rank_;

    std::unique_ptr<char[]> dec_buffer_;
    mutable size_t dec_last_; // nb: cache to avoid re-spelling of the same phrase on subsequent queries -- modified in a const function
    mutable size_t dec_last_len_;

    struct PhraseContaining {
        size_t phrase;
        size_t start;
    };

    PhraseContaining phrase_containing(size_t const i) const {
        // query what phrase contains position i of the original input
        PhraseContaining r;
        r.phrase = start_rank_(i) - 1;
        assert(r.phrase != SIZE_MAX);

        // find the starting position of that phrase
        // nb: naive select query, but since start_ is expected to be dense, we shouldn't have to scan for long
        auto start = word_packing::bit_accessor(start_.get());
        size_t j = i;
        while(!start[j]) --j;
        r.start = j;

        return r;
    }

public:
    TopKAccess(std::filesystem::path const& path, size_t const k)
        : n_(std::filesystem::file_size(path)),
          k_(k),
          parsing_(0, std::bit_width(k-1)) {
        
        // initialize phrase start bit vector
        {
            auto const num_packs = word_packing::num_packs_required<Pack>(n_, 1);
            start_ = std::make_unique<Pack[]>(num_packs);
            for(size_t i = 0; i < num_packs; i++) {
                start_[i] = 0;
            }
        }
        auto start = word_packing::bit_accessor(start_.get());

        iopp::FileInputStream in(path);

        // compute trie
        trie_ = topk_twopass::compute_topk(in.begin(), in.end(), k, k >> 8);

        // compute parsing and mark phrase starts
        size_t i = 0;
        height_ = 0;

        in.seekg(0, std::ios::beg);
        topk_twopass::parse(in.begin(), in.end(), trie_, [&](topk_twopass::Phrase f) {
            height_ = std::max(height_, f.len);

            if(f.is_literal()) {
                literal_.push_back(1);
                parsing_.push_back((uint8_t)f.literal);
            } else {
                literal_.push_back(0);
                parsing_.push_back(f.node);
            }

            start[i] = 1;
            i += f.len;
        });
        assert(i == n_);

        parsing_.shrink_to_fit();
        literal_.shrink_to_fit();

        // compute rank/select data structure on start
        start_rank_ = Rank(start_.get(), n_);
        assert(start_rank_(n_ - 1) == parsing_.size());
        
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
                dec_last_len_ = trie_.spell_reverse(v, dec_buffer_.get());
                dec_last_ = v;
            }
        
            auto const offs = (dec_last_len_ - 1) - (i - p.start);
            return dec_buffer_[offs];
        }
    }

    void extract(size_t const i, size_t const len, char* buffer) const {
        // TODO: implement substrings extraction
    }

    size_t lce(size_t const i, size_t const j) const {
        // TODO: implement LCE query
        return 0;
    }

    size_t num_phrases() const {
        return parsing_.size();
    }

    size_t height() const {
        return height_;
    }

    size_t length() const {
        return n_;
    }
};
