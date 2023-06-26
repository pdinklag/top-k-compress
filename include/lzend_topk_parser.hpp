#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <display.hpp>
#include <tdc/util/concepts.hpp>

#include <lzend_phrase.hpp>

// computes the LZ-End parsing of an input according to [Kempa & Kosolobov, 2017]
// using a trie on the reversed LZ-End phrases as well as a local index similar to that of [Kreft & Navarro, 2015]
template<bool prefer_local_, typename Trie_, typename WindowIndex_, std::unsigned_integral Index = uint32_t>
class LZEndTopkParser {
public:
    using Trie = Trie_;
    using WindowIndex = WindowIndex_;

    using Phrase = LZEndPhrase<char, Index>;

    struct Stats {
        WindowIndex::MemoryProfile max_window_memory;
        size_t phrases_from_trie;
        size_t phrases_from_trie_total_len;
    };

private:
    static constexpr bool DEBUG = false;

    static constexpr Index NIL = 0;

    // global state
    size_t max_block_; // the maximum size of a block (input window size)

    Index z_;          // the current number of LZ-End phrases
    Index ztrie_;      // the current number of LZ-End phrases that have been inserted into the trie (<= z)
    Index ztrie_end_;  // the ending position of the last LZ-End phrases entered into the trie.

    Index trie_ref_offs_;
    Trie* trie_;
    std::unique_ptr<Phrase[]> local_phrases_;

    // block state
    std::string buffer_;

    // stats
    Stats stats_;

    bool absorb_two_trie(Index const m, Index const p, Index const len1, Index const len2) const {
        return p > 0 && m > len1 && len2 < max_block_ && trie_->depth(p) >= len2;
    }

    bool absorb_one_trie(Index const m, Index const p, Index const len1) const {
        return p > 0 && m > 0 && len1 < max_block_ && trie_->depth(p) >= len1;
    }

    void precompute_absorb_local(Index const m, Index const len1, Index const len2, WindowIndex const& windex, Index& lce1, Index& lnk1, Index& lce2, Index& lnk2) const {
        lce1 = 0, lnk1 = 0, lce2 = 0, lnk2 = 0;
        if(m > 0 && len1 < max_block_) {
            if(m > len1 && len2 < max_block_) {
                // compute both absorbOne2 and absorbTwo2
                windex.marked_lcp2(m-1, z_-1, lnk1, lce1, lnk2, lce2);
            } else {
                // compute just absorbOne2
                windex.marked_lcp(m-1, lnk1, lce1);
            }
        }
    }

    // a.k.a. absorbTwo2 in [KK, 2017]
    bool absorb_two_local(Index const m, Index const len1, Index const len2, Index const lce2) const {
        return m > len1 && len2 < max_block_ && lce2 >= len2;
    }

    // a.k.a. absorbTwo2 in [KK, 2017]
    bool absorb_one_local(Index const m, Index const len1, Index const lce1) const {
        return m > 0 && lce1 >= len1;
    }

    void parse_block(
        size_t const phase,
        std::string_view window,
        Index const curblock_size,
        Index const window_begin_glob,
        Index const curblock_window_offs,
        bool const final_block) {

        if constexpr(DEBUG) {
            std::cout << "curblock_size=" << curblock_size << ", curblock_window_offs=" << curblock_window_offs << ", window.size()=" << window.size() << std::endl;
            std::cout << "computing index..." << std::endl;
        }

        // construct window index
        WindowIndex windex(window);
        auto const& rfp = windex.reverse_fingerprints();

        // preprocess: mark positions of phrases that end in the previous two blocks within the window
        if(phase >= 1) {
            if constexpr(DEBUG) std::cout << "preprocessing next block..." << std::endl;

            // scan
            Index x = z_;
            Index xend = phase * max_block_ - 1; // the ending position of the x-th LZ-End phrase
            while(x > 0 && xend >= window_begin_glob) {
                xend -= local_phrases_[x].len;
                --x;
            }

            // completely discard phrases beginning before the current window
            Index const discard = x;
            if constexpr(DEBUG) std::cout << "\tdiscarding " << discard << " phrases that end before the window" << std::endl;

            if(discard > 0) {
                assert(z_ >= discard);     // we must have introduced this many phrases
                assert(ztrie_ >= discard); // they must also have been persisted in the trie

                // remove by shifting and reducing size
                for(Index i = 0; i < z_ - discard; i++) {
                    assert(!is_trie_ref(1 + i + discard));
                    local_phrases_[1 + i] = local_phrases_[1 + i + discard];
                }

                z_ -= discard;
                ztrie_ -= discard;
            }

            // re-number phrases and mark
            for(x = 1; x <= z_; x++) {
                xend += local_phrases_[x].len;

                assert(xend >= window_begin_glob);
                windex.mark(xend - window_begin_glob, x, true);
            }
            assert(xend == phase * max_block_ - 1); // we should end up back at the final position of the previous block
        }

        // begin LZEnd algorithm by [Kempa & Kosolobov, 2017]
        for(Index mblock = 0; mblock < curblock_size; mblock++) {
            auto const m = curblock_window_offs + mblock;  // the current position within the window
            auto const mglob = window_begin_glob + m; // the current global position in the input
            char const next_char = window[m];

            if constexpr(DEBUG) {
                std::cout << std::endl;
                std::cout << "--- mblock=" << mblock << " -> mglob=" << mglob << " ---" << std::endl;
                std::cout << "next character: " << display(next_char) << std::endl;
            }

            Index p = 0;
            if(phase >= 2) {
                auto const rsuf_begin = windex.pos_to_reverse(m-1);
                auto const rsuf_len = windex.size() - 1 - rsuf_begin;
                p = trie_->find_prefix(rfp, rsuf_begin, rsuf_len);
            }

            auto const len1 = local_phrases_[z_].len;                         // length of the current phrase
            auto const len2 = len1 + (z_ > 0 ? local_phrases_[z_-1].len : 0); // total length of the two current phrases

            // sanity
            #ifndef NDEBUG
            if(m > 0) assert(windex.is_marked(m-1));
            if(m > len1 && z_ > 1) assert(windex.is_marked(m-1-len1));
            #endif

            // combined precomputation of absorbOne2 and absorbTwo2
            // even though absorbOne2 is never needed if absorbTwo2 returns true,
            // the result of absorbOne2 can be used to compute absorbTwo2 without the need for temporarily unmarking and re-marking a phrase
            // furthermore, the number of predecessor and successor queries is minimized this way
            Index lce_trie, lnk_trie;
            Index lce1, lnk1; // corresponds to absorbOne2
            Index lce2, lnk2; // corresponds to absorbTwo2
            
            // figure out what case we are dealing with
            enum AlgorithmCase {
                ABSORB_ONE_TRIE,
                ABSORB_ONE_LOCAL,
                ABSORB_TWO_TRIE,
                ABSORB_TWO_LOCAL,
                NEW_CHAR
            };

            AlgorithmCase whence;

            if constexpr(prefer_local_) {
                precompute_absorb_local(m, len1, len2, windex, lce1, lnk1, lce2, lnk2);
                if(absorb_two_local(m, len1, len2, lce2)) {
                    whence = AlgorithmCase::ABSORB_TWO_LOCAL;
                } else if(absorb_two_trie(m, p, len1, len2)) {
                    whence = AlgorithmCase::ABSORB_TWO_TRIE;
                } else if(absorb_one_local(m, len1, lce1)) {
                    whence = AlgorithmCase::ABSORB_ONE_LOCAL;
                } else if(absorb_one_trie(m, p, len1)) {
                    whence = AlgorithmCase::ABSORB_ONE_TRIE;
                } else {
                    whence = AlgorithmCase::NEW_CHAR;
                }
            } else {
                if(absorb_two_trie(m, p, len1, len2)) {
                    whence = AlgorithmCase::ABSORB_TWO_TRIE;
                } else {
                    precompute_absorb_local(m, len1, len2, windex, lce1, lnk1, lce2, lnk2);
                    if(absorb_two_local(m, len1, len2, lce2)) {
                        whence = AlgorithmCase::ABSORB_TWO_LOCAL;
                    } else if(absorb_one_trie(m, p, len1)) {
                        whence = AlgorithmCase::ABSORB_ONE_TRIE;
                    } else if(absorb_one_local(m, len1, lce1)) {
                        whence = AlgorithmCase::ABSORB_ONE_LOCAL;
                    } else {
                        whence = AlgorithmCase::NEW_CHAR;
                    }
                }
            }

            if(whence == AlgorithmCase::ABSORB_TWO_TRIE || whence ==  AlgorithmCase::ABSORB_TWO_LOCAL) {
                // merge the two current phrases and extend their length by one
                if constexpr(DEBUG) std::cout << "\tMERGE phrases " << z_ << " and " << z_-1 << " to new phrase of length " << (len2+1) << std::endl;

                // updateRecent: unregister current phrase
                windex.unmark(m - 1);

                // updateRecent: unregister previous phrase
                windex.unmark(m - 1 - len1);

                // delete current phrase
                --z_;
                assert(z_); // nb: must still have at least phrase 0

                if(whence == AlgorithmCase::ABSORB_TWO_LOCAL) {
                    // we are here because of absorbTwo2 (local index), use precomputed link
                    p = lnk2;
                    assert(!is_trie_ref(p));
                } else {
                    // we are here because of absorbTwo (trie)
                    p += trie_ref_offs_;
                }

                // merge phrases
                local_phrases_[z_] = Phrase { p, len2 + 1, next_char };
            } else if(whence == AlgorithmCase::ABSORB_ONE_TRIE || whence == AlgorithmCase::ABSORB_ONE_LOCAL) {
                // extend the current phrase by one character
                if constexpr(DEBUG) std::cout << "\tEXTEND phrase " << z_ << " to length " << (len1+1) << std::endl;

                // updateRecent: unregister current phrase
                windex.unmark(m - 1);

                if(whence == AlgorithmCase::ABSORB_ONE_LOCAL) {
                    // we are here because of absorbOne2 (local index), use precomputed link
                    p = lnk1;
                    assert(!is_trie_ref(p));
                } else {
                    // we are here because of absorbOne (trie)
                    p += trie_ref_offs_;
                }

                // extend phrase
                local_phrases_[z_] = Phrase { p, len1 + 1, next_char };
            } else {
                // begin a new phrase of initially length one
                if constexpr(DEBUG) std::cout << "\tNEW phrase " << (z_+1) << " of length 1" << std::endl;
                
                ++z_;
                local_phrases_[z_] = Phrase { p, 1, next_char };
            }

            if constexpr(DEBUG) std::cout << "\t-> z=" << z_ << ", link=" << local_phrases_[z_].link << ", len=" << local_phrases_[z_].len << ", last=" << display(local_phrases_[z_].last) << std::endl;

            // update lens
            assert(local_phrases_[z_].len <= max_block_);

            // updateRecent: register updated current phrase
            windex.mark(m, z_);
            assert(windex.is_marked(m));
        }

        stats_.max_window_memory = WindowIndex::MemoryProfile::max(stats_.max_window_memory, windex.memory_profile());

        if(phase >= 1 && !final_block) {
            if constexpr(DEBUG) {
                std::cout << std::endl;
                std::cout << "postprocessing ..." << std::endl;
            }

            // insert phrases that end in the first two blocks within the window into the trie
            // while doing that, also recompute marked to contain only phrases that are inserted
            windex.clear_marked();

            if constexpr(DEBUG) {
                std::cout << "inserting phrases ending in sliding block into trie ..." << std::endl;
            }

            Index const ztrie_before_inserts = ztrie_;
            Index const border = window_begin_glob + curblock_window_offs;
            while(ztrie_ < z_ && ztrie_end_ + local_phrases_[ztrie_].len <= border) { // we go one phrase beyond the border according to [KK, 2017]
                // the phrase may be emitted
                if(on_emit_phrase) on_emit_phrase(local_phrases_[ztrie_]);

                // we enter phrases[ztrie]
                ztrie_end_ += local_phrases_[ztrie_].len;

                // count phrases that we introduced thanks to the trie
                if(is_trie_ref(local_phrases_[ztrie_].link)) {
                    ++stats_.phrases_from_trie;
                    stats_.phrases_from_trie_total_len += local_phrases_[ztrie_].len;
                }

                // insert into trie
                Index const rend = windex.pos_to_reverse(ztrie_end_ - window_begin_glob);
                Index const rlen = windex.size() - 1 - rend;

                trie_->insert(rfp, rend, rlen, max_block_);

                // mark the phrase end for postprocessing of lnks
                windex.mark(ztrie_end_ - window_begin_glob, ztrie_, true);

                ++ztrie_;
            }
        }

        if(final_block) {
            // emit remaining phrases
            for(auto i = ztrie_; i <= z_; i++) {
                if(on_emit_phrase) on_emit_phrase(local_phrases_[i]);

                if(is_trie_ref(local_phrases_[i].link)) {
                    ++stats_.phrases_from_trie;
                    stats_.phrases_from_trie_total_len += local_phrases_[i].len;
                }
            }
        }
    }

public:
    LZEndTopkParser(size_t const max_block, Trie& trie)
        : max_block_(max_block),
          z_(0),
          ztrie_(1),
          ztrie_end_(-1), // the empty phrase ends at position -1
          trie_ref_offs_(3 * max_block),
          trie_(&trie),
          buffer_(3 * max_block, 0),
          local_phrases_(std::make_unique<Phrase[]>(3 * max_block)) {

        local_phrases_[0] = Phrase { 0, 0, 0 };
        stats_.phrases_from_trie = 0;
        stats_.phrases_from_trie_total_len = 0;
    }

    // callbacks
    std::function<void(Phrase const&)> on_emit_phrase;

    template<tdc::InputIterator<char> In>
    void parse(In begin, In const& end) {
        std::string_view window;
        size_t phase = 0;
        while(begin != end) {
            if constexpr(DEBUG) {
                std::cout << std::endl;
                std::cout << "=== phase " << phase << " ===" << std::endl;
            }

            // slide previous two blocks
            for(size_t i = 0; i < 2 * max_block_; i++) {
                buffer_[i] = buffer_[max_block_ + i];
            }

            // read next block
            Index const curblock_buffer_offs = 2 * max_block_;
            Index curblock_size = 0;
            {
                while(begin != end && curblock_size < max_block_) {
                    buffer_[curblock_buffer_offs + curblock_size] = *begin++;
                    ++curblock_size;
                }

                // window = std::string_view(buffer.at(offs), buffer.at(offs + num_read));
            }

            // determine window and block boundaries
            Index window_begin_glob;
            Index curblock_window_offs;
            switch(phase) {
                case 0:
                    // in the very first phase, we only deal with a single block
                    window = std::string_view(buffer_.data() + 2 * max_block_, curblock_size);
                    window_begin_glob = 0;
                    curblock_window_offs = 0;
                    break;
                
                case 1:
                    // in the second phase, we additionally deal with one previous block
                    window = std::string_view(buffer_.data() + max_block_, max_block_ + curblock_size);
                    window_begin_glob = 0;
                    curblock_window_offs = max_block_;
                    break;
                
                default:
                    // in subsequent phases, the window spans all 3 blocks
                    window = std::string_view(buffer_.data(), 2 * max_block_ + curblock_size);
                    window_begin_glob = (phase - 2) * max_block_;
                    curblock_window_offs = 2 * max_block_;
                    break;
            }

            // parse block
            parse_block(phase, window, curblock_size, window_begin_glob, curblock_window_offs, begin == end);

            // advance to next phase
            ++phase;
        }
    }

    bool is_trie_ref(Index const p) const { return p >= trie_ref_offs_; }

    Index const num_phrases() const { return z_; }
    Trie const& trie() const { return *trie_; }
    Stats const& stats() const { return stats_; }

    struct MemoryProfile {
        size_t buffer;
        size_t parsing;
        size_t phrase_hashes;
        size_t lnks_lens;

        size_t total() const { return buffer + parsing + phrase_hashes + lnks_lens; }
    };

    MemoryProfile memory_profile() const {
        MemoryProfile profile;
        profile.buffer = buffer_.capacity() * sizeof(char);
        profile.parsing = 3 * max_block_ * sizeof(Phrase);
        profile.phrase_hashes = 0;
        profile.lnks_lens = 0;
        return profile;
    }
};
