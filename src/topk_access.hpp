#include "topk_twopass_impl.hpp"

#include <bit>
#include <filesystem>

#include <iopp/file_input_stream.hpp>
#include <ordered/btree/set.hpp>
#include <ordered/range_marking/set.hpp>
#include <word_packing.hpp>

#include <idiv_ceil.hpp>
#include <small_trie.hpp>

class TopKAccess {
private:
    using Pack = uintmax_t;

    size_t n_;
    size_t k_;
    SmallTrie<true> trie_;
    size_t height_;
    word_packing::PackedIntVector<Pack> parsing_;
    word_packing::BitVector literal_;
    std::unique_ptr<char[]> dec_buffer_;
    std::unique_ptr<Pack[]> start_;

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

        parsing_.shrink_to_fit();
        literal_.shrink_to_fit();

        // TODO: compute rank/select data structure on start

        // initialize decode buffer
        dec_buffer_ = std::make_unique<char[]>(height_);
    }

    char operator[](size_t const i) const {
        // TODO: implement single-character access
        return 0;
    }

    void extract(size_t const i, size_t const len, char* buffer) const {
        // TODO: implement substrings extraction
    }

    size_t lce(size_t const i, size_t const j) const {
        // TODO: implement LCE query
    }

    size_t num_phrases() const {
        return parsing_.size();
    }

    size_t mem_size() const {
        return sizeof(TopKAccess) +
            idiv_ceil(parsing_.capacity() * parsing_.width(), 64) * sizeof(Pack) +
            idiv_ceil(literal_.capacity(), 64) * sizeof(Pack) +
            idiv_ceil(n_, 8);
            height_;
    }
};
