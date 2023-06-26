#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <display.hpp>
#include <tdc/util/concepts.hpp>

// computes the LZ-End parsing of an input according to [Kempa & Kosolobov, 2017]
// using a trie on the reversed LZ-End phrases as well as a local index similar to that of [Kreft & Navarro, 2015]
template<bool prefer_local_, typename Parsing_, typename Trie_, typename WindowIndex_, std::unsigned_integral Index = uint32_t>
class LZEndKKParser {
public:
    using Parsing = Parsing_;
    using Trie = Trie_;
    using WindowIndex = WindowIndex_;

    struct Stats {
        WindowIndex::MemoryProfile max_window_memory;
        size_t phrases_from_trie;
        size_t phrases_from_trie_total_len;
    };

private:
    static constexpr bool DEBUG = false;
    static constexpr bool PARANOID = false;

    static constexpr Index NIL = 0;

    // global state
    size_t max_block_; // the maximum size of a block (input window size)
    Index z_;          // the current number of LZ-End phrases
    Index ztrie_;      // the current number of LZ-End phrases that have been inserted into the trie (< z)
    Index ztrie_end_;  // the ending position of the last LZ-End phrases entered into the trie.

    Parsing phrases_;
    std::vector<uint64_t> phrase_hashes_;
    Trie trie_;

    // block state
    std::unique_ptr<Index[]> lnks_;
    std::unique_ptr<Index[]> lens_;

    std::string buffer_;

    // stats
    Stats stats_;

    // a.k.a. commonPart in [KK, 2017]
    bool common_part_in_trie(Index const m, Index const p, Index const len, WindowIndex const& windex, Index const window_begin_glob) const {
        auto const plen = phrases_[p].len;
        if(p > 0 && m > plen) {
            if constexpr(DEBUG) {
                std::cout << "\ttesting whether trie phrases " << p << " and " << (p-1) << " have a common suffix of length " << len << " with current input suffix" << std::endl;
            }
            assert(plen > 0);

            if(plen < len) {
                auto const pos = m - plen;
                auto const lhash = windex.reverse_fingerprint(pos, m-1);
                if(phrase_hashes_[p] == lhash) {
                    if(lens_[pos] - 1 + plen == len) {
                        if(lnks_[pos] != NIL) {
                            auto const nca_len = trie_.nca_len(lnks_[pos], p-1);
                            if(nca_len + plen >= len) {
                                if constexpr(DEBUG) {
                                    std::cout << "\t\tTRUE - combined length of NCA and phrase matches" << std::endl;
                                }
                                return true;
                            } else {
                                if constexpr(DEBUG) {
                                    std::cout << "\t\tFALSE - combined length of NCA and phrase do not match" << std::endl;
                                }
                            }
                        } else {
                            if constexpr(DEBUG) {
                                auto const posglob = window_begin_glob + pos;                         
                                std::cout << "\t\tFALSE - lnks[" << posglob << "] is NIL" << std::endl;
                            }
                        }
                    } else {
                        if constexpr(DEBUG) {
                            auto const posglob = window_begin_glob + pos;                         
                            std::cout << "\t\tFALSE - combined length of trie phrase " << p << " (" << plen << ") and lens[" << posglob << "]=" << lens_[pos] << " minus 1 does not match" << std::endl;
                        }
                    }
                } else {
                    if constexpr(DEBUG) {
                        std::cout << "\t\tFALSE - trie phrase hash (0x" << std::hex << phrase_hashes_[p] << " does not match current suffix hash (0x" << lhash << std::dec << ")" << std::endl;
                    }
                }
            } else {
                if constexpr(DEBUG) {
                    std::cout << "\t\tFALSE - trie phrase " << p << " is already too long by itself" << std::endl;
                }
            }
        }
        return false;
    }

    // a.k.a. absorbTwo in [KK, 2017]
    bool absorb_two_trie(Index const m, Index const p, Index const len2, WindowIndex const& windex, Index const window_begin_glob) const {
        if(len2 < max_block_ && common_part_in_trie(m, p, len2, windex, window_begin_glob)) {
            if constexpr(DEBUG) {
                std::cout << "\tabsorbTwo returned true" << std::endl;
            }
            return true;
        } else {
            return false;
        }
    }

    // a.k.a. absorbOne in [KK, 2017]
    bool absorb_one_trie(Index const m, Index const p, Index const len1, WindowIndex const& windex, Index const window_begin_glob) const {
        if(p > 0 && m > 0 && len1 < max_block_) {
            if constexpr(DEBUG) {
                std::cout << "\ttesting whether trie phrase " << p << " has a common suffix with current phrase of length " << len1 << std::endl;
            }
            auto const plen = phrases_[p].len;
            if(plen < len1) {
                if constexpr(DEBUG) {
                    std::cout << "\t\ttrie phrase " << p << " is shorter than current phrase, delegating" << std::endl;
                }
                if(common_part_in_trie(m, p, len1, windex, window_begin_glob)) {
                    if constexpr(DEBUG) {
                        std::cout << "\tabsorbOne returned true" << std::endl;
                    }
                    return true;
                }
            } else {
                if(phrases_[p].last == phrases_[z_].last) {
                    if(len1 > 1 && lnks_[m-1] == NIL) {
                        if constexpr(DEBUG) {
                            std::cout << "\t\tFALSE - a phrase ends at the previous position, but the trie link is NIL" << std::endl;
                        }
                    } else {
                        if(len1 == 1) {
                            if constexpr(DEBUG) {
                                std::cout << "\t\tTRUE - last character matches for length-1 phrase" << std::endl;
                                std::cout << "\tabsorbOne returned true" << std::endl;
                            }
                            return true;
                        } else {
                            auto const nca_len = trie_.nca_len(lnks_[m-1], phrases_[p].link);
                            if(nca_len + 1 >= len1) {
                                if constexpr(DEBUG) {
                                    std::cout << "\t\tTRUE - NCA length plus 1 exceeds current phrase length" << std::endl;
                                    std::cout << "\tabsorbOne returned true" << std::endl;
                                }
                                return true;
                            } else  {
                                if constexpr(DEBUG) {
                                    std::cout << "\t\tFALSE - the NCA length plus 1 is too short" << std::endl;
                                }
                            }
                        }
                    }
                } else {
                    if constexpr(DEBUG) {
                        std::cout << "\t\tFALSE: end characters do not match" << std::endl;
                    }
                }
            }
        }
        return false;
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
    };

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
            if constexpr(DEBUG) {
                std::cout << "preprocessing next block..." << std::endl;
            }

            Index x = z_;
            Index xend = phase * max_block_ - 1; // the ending position of the x-th LZ-End phrase
            while(x > 0 && xend >= window_begin_glob) {
                windex.mark(xend - window_begin_glob, x, true);
                xend -= phrases_[x].len;
                --x;
            }
        }

        // begin LZEnd algorithm by [Kempa & Kosolobov, 2017]
        for(Index mblock = 0; mblock < curblock_size; mblock++) {
            auto const m = curblock_window_offs + mblock;  // the current position within the window
            auto const mglob = window_begin_glob + m; // the current global position in the input
            char const next_char = window[m];

            if constexpr(DEBUG) {
                std::cout << std::endl;
                std::cout << "--- mblock=" << mblock << " -> mglob=" << mglob << " ---" << std::endl;
                std::cout << "next character: " << display(window[mblock]) << std::endl;
            }

            Index p = 0;
            if(phase >= 2) {
                auto const rsuf_begin = windex.pos_to_reverse(m-1);
                auto const rsuf_len = windex.size() - 1 - rsuf_begin;

                Index hash_match;
                p = trie_.approx_find_phr(rfp, rsuf_begin, rsuf_len, hash_match);

                #ifndef NDEBUG
                if constexpr(PARANOID) {
                    if(p > 0 && hash_match > 0) {
                        std::string rsuf;
                        rsuf.reserve(hash_match);
                        phrases_.decode_rev(std::back_inserter(rsuf), p, hash_match);
                        
                        auto const* rstr = rfp.data() + rsuf_begin;
                        for(size_t i = 0; i < hash_match; i++) {
                            assert(rstr[i] == rsuf[i]);
                        }
                    }
                }
                #endif
            }

            lnks_[m] = NIL;
            auto const len1 = phrases_[z_].len;                        // length of the current phrase
            auto const len2 = len1 + (z_ > 0 ? phrases_[z_-1].len : 0); // total length of the two current phrases

            // sanity
            #ifndef NDEBUG
            if(m > 0) assert(windex.is_marked(m-1));
            if(m > len1 && z_ > 1) assert(windex.is_marked(m-1-len1));
            #endif

            // combined precomputation of absorbOne2 and absorbTwo2
            // even though absorbOne2 is never needed if absorbTwo2 returns true,
            // the result of absorbOne2 can be used to compute absorbTwo2 without the need for temporarily unmarking and re-marking a phrase
            // furthermore, the number of predecessor and successor queries is minimized this way
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
                } else if(absorb_two_trie(m, p, len2, windex, window_begin_glob)) {
                    whence = AlgorithmCase::ABSORB_TWO_TRIE;
                } else if(absorb_one_local(m, len1, lce1)) {
                    whence = AlgorithmCase::ABSORB_ONE_LOCAL;
                } else if(absorb_one_trie(m, p, len1, windex, window_begin_glob)) {
                    whence = AlgorithmCase::ABSORB_ONE_TRIE;
                } else {
                    whence = AlgorithmCase::NEW_CHAR;
                }
            } else {
                if(absorb_two_trie(m, p, len2, windex, window_begin_glob)) {
                    whence = AlgorithmCase::ABSORB_TWO_TRIE;
                } else {
                    precompute_absorb_local(m, len1, len2, windex, lce1, lnk1, lce2, lnk2);
                    if(absorb_two_local(m, len1, len2, lce2)) {
                        whence = AlgorithmCase::ABSORB_TWO_LOCAL;
                    } else if(absorb_one_trie(m, p, len1, windex, window_begin_glob)) {
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
                phrases_.pop_back();
                phrase_hashes_.pop_back();
                --z_;
                assert(z_); // nb: must still have at least phrase 0

                if(whence == AlgorithmCase::ABSORB_TWO_LOCAL) {
                    // we are here because of absorbTwo2 (local index), use precomputed link
                    p = lnk2;
                } else {
                    // we are here because of absorbTwo (trie)
                    lnks_[m] = p;
                    if constexpr(DEBUG) std::cout << "\tsetting lnks[" << mglob << "] := " << p << std::endl;
                }

                // merge phrases
                phrases_.replace_back(p, len2 + 1, next_char);
            } else if(whence == AlgorithmCase::ABSORB_ONE_TRIE || whence == AlgorithmCase::ABSORB_ONE_LOCAL) {
                // extend the current phrase by one character
                if constexpr(DEBUG) std::cout << "\tEXTEND phrase " << z_ << " to length " << (len1+1) << std::endl;

                // updateRecent: unregister current phrase
                windex.unmark(m - 1);

                if(whence == AlgorithmCase::ABSORB_ONE_LOCAL) {
                    // we are here because of absorbOne2 (local index), use precomputed link
                    p = lnk1;
                } else {
                    // we are here because of absorbOne (trie)
                    lnks_[m] = p;
                    if constexpr(DEBUG) std::cout << "\tsetting lnks[" << mglob << "] := " << p << std::endl;
                }

                // extend phrase
                phrases_.replace_back(p, len1 + 1, next_char);
            } else {
                // begin a new phrase of initially length one
                if constexpr(DEBUG) std::cout << "\tNEW phrase " << (z_+1) << " of length 1" << std::endl;
                
                ++z_;
                phrases_.emplace_back(p, 1, next_char);
                phrase_hashes_.emplace_back(0);
            }

            if constexpr(DEBUG) std::cout << "\t-> z=" << z_ << ", link=" << phrases_[z_].link << ", len=" << phrases_[z_].len << ", last=" << display(phrases_[z_].last) << std::endl;

            // update lens
            assert(phrases_[z_].len <= max_block_);
            lens_[m] = phrases_[z_].len;

            #ifndef NDEBUG
            if constexpr(PARANOID) {
                // verify that the current phrase is a valid LZEnd phrase
                if(phrases_[z_].len > 1) {
                    auto const lnk = phrases_[z_].link;
                    assert(lnk > 0);
                    auto const common_len = phrases_[z_].len - 1;

                    std::string rsuf;
                    rsuf.reserve(common_len);
                    phrases_.decode_rev(std::back_inserter(rsuf), lnk, common_len);
                    
                    auto const offs = windex.pos_to_reverse(m-1);
                    auto const* rstr = rfp.data() + offs;
                    for(size_t i = 0; i < common_len; i++) {
                        assert(rstr[i] == rsuf[i]);
                    }
                }
            }
            #endif

            // update phrase hash
            phrase_hashes_[z_] = windex.reverse_fingerprint(m - phrases_[z_].len + 1, m);

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
            while(ztrie_ < z_ && ztrie_end_ + phrases_[ztrie_].len <= border) { // we go one phrase beyond the border according to [KK, 2017]
                // we enter phrases[ztrie]
                ztrie_end_ += phrases_[ztrie_].len;

                // count phrases that we introduced thanks to the trie
                if(phrases_[ztrie_].len > 1 && phrases_[ztrie_].link <= ztrie_before_inserts) {
                    ++stats_.phrases_from_trie;
                    stats_.phrases_from_trie_total_len += phrases_[ztrie_].len;
                }

                // insert into trie
                Index const rend = windex.pos_to_reverse(ztrie_end_ - window_begin_glob);
                Index const rlen = windex.size() - 1 - rend;

                trie_.insert(rfp, rend, rlen, max_block_);

                // mark the phrase end for postprocessing of lnks
                windex.mark(ztrie_end_ - window_begin_glob, ztrie_, true);

                ++ztrie_;
            }

            // update lnks
            if constexpr(DEBUG) {
                std::cout << "postprocessing lnks ..." << std::endl;
            }
            for(Index i = 0; i < curblock_size; i++) {
                auto const q = curblock_window_offs + i;  // the current position within the window

                // update if necessary
                if(lens_[q] > 1 && lnks_[q] == NIL) { 
                    Index x, ln;
                    windex.marked_lcp(q-1, x, ln);
                    if(lens_[q] <= ln + 1) { // nb: +1 because the phrase length includes the final as well -- this appears to be a mistake in [KK, 2017]
                        assert(x <= ztrie_); // we cannot refer to a phrase that is not yet in the trie
                        lnks_[q] = x;

                        if constexpr(DEBUG) {
                            auto const qglob = window_begin_glob + q;
                            std::cout << "\tsetting lnks[" << qglob << "] := " << x << std::endl;
                        }
                        // nb: lens isn't updated apparently -- at the time of writing, I'd lie if I knew why
                    } else {
                        if constexpr(DEBUG) {
                            auto const qglob = window_begin_glob + q;
                            std::cout << "\tleaving lnks[" << qglob << "] at NIL because ln=" << ln << " (with phrase x=" << (ln > 0 ? x : 0) << ") plus 1 is less than lens[" << qglob << "]=" << lens_[q] << std::endl;
                        }
                    }
                } else {
                    if constexpr(DEBUG) {
                        auto const qglob = window_begin_glob + q;
                        std::cout << "\tleaving lnks[" << qglob << "] = " << lnks_[q] << ", lens[" << qglob << "] = " << lens_[q] << std::endl;
                    }
                }
            }
        }

        if(final_block) {
            for(auto i = ztrie_; i < z_; i++) {
                if(phrases_[i].len > 0 && phrases_[i].link <= ztrie_) {
                    ++stats_.phrases_from_trie;
                    stats_.phrases_from_trie_total_len += phrases_[i].len;
                }
            }
        }
    }

public:
    LZEndKKParser(size_t const max_block)
        : max_block_(max_block),
          z_(0),
          ztrie_(1),
          ztrie_end_(-1), // the empty phrase ends at position -1
          trie_(phrases_),
          lnks_(std::make_unique<Index[]>(3 * max_block)),
          lens_(std::make_unique<Index[]>(3 * max_block)),
          buffer_(3 * max_block, 0) {

        phrase_hashes_.emplace_back(0); // phrase 0

        for(size_t i = 0; i < 3 * max_block; i++) {
            lnks_[i] = NIL;
            lens_[i] = 0;
        }

        stats_.phrases_from_trie = 0;
        stats_.phrases_from_trie_total_len = 0;
    }

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
                lens_[i] = lens_[max_block_ + i];
                lnks_[i] = lnks_[max_block_ + i];
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

    Index const num_phrases() const { return z_; }
    Parsing const& parsing() const { return phrases_; }
    Trie const& trie() const { return trie_; }
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
        profile.parsing = phrases_.memory_size();
        profile.phrase_hashes = phrase_hashes_.capacity() * sizeof(uint64_t);
        profile.lnks_lens = 2 * (sizeof(Index) * 3 * max_block_);
        return profile;
    };
};
