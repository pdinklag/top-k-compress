#include "topk_common.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <display.hpp>

#include <lzend_parsing.hpp>
#include <lzend_window_index.hpp>
#include <topk_lzend_trie.hpp>

#include <code/concepts.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'Z') << 24 |
    ((uint64_t)'E') << 16 |
    ((uint64_t)'N') << 8 |
    ((uint64_t)'D');

constexpr bool PROTOCOL = false;
constexpr bool DEBUG = false;

using Index = uint32_t;

constexpr TokenType TOK_REF = 0;
constexpr TokenType TOK_LEN = 1;
constexpr TokenType TOK_LITERAL = 2;

void setup_encoding(BlockEncodingBase& enc, size_t const k, bool const use_trie) {
    enc.register_binary(k-1, use_trie); // TOK_REF
    enc.register_huffman();             // TOK_LEN
    enc.register_huffman();             // TOK_LITERAL
}

// computes the LZ-End parsing of an input using a modified version of the [Kempa & Kosolobov, 2017] algorithm
// replacing their compact trie by a top-k trie
// setting "use_trie_" to "false" effectively sets k := 0
template<bool use_trie_ = true>
class LZEndTopkParser {
public:
    using Trie = TopKLZEndTrie<Index>;
    using WindowIndex = LZEndWindowIndex<Index>;

    using Phrase = LZEndPhrase<char, Index>;

    struct Stats {
        WindowIndex::MemoryProfile max_window_memory;
        size_t phrases_from_trie;
        size_t phrases_from_trie_total_len;
    };

private:
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
        if constexpr(use_trie_) {
            return p > 0 && m > len1 && len2 < max_block_ && trie_->depth(p) >= len2;
        } else {
            return false;
        }
    }

    bool absorb_one_trie(Index const m, Index const p, Index const len1) const {
        if constexpr(use_trie_) {
            return p > 0 && m > 0 && len1 < max_block_ && trie_->depth(p) >= len1;
        } else {
            return false;
        }
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

        // begin modified LZEnd algorithm based on [Kempa & Kosolobov, 2017]
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
            if constexpr(use_trie_) {
                if(phase >= 2) {
                    auto const rsuf_begin = windex.pos_to_reverse(m-1);
                    auto const rsuf_len = windex.size() - 1 - rsuf_begin;
                    p = trie_->find_prefix(rfp, rsuf_begin, rsuf_len);
                }
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
            if constexpr(use_trie_) {
                precompute_absorb_local(m, len1, len2, windex, lce1, lnk1, lce2, lnk2);

                auto const _absorb_two_local = absorb_two_local(m, len1, len2, lce2);
                auto const _absorb_two_trie = absorb_two_trie(m, p, len1, len2);
                if(_absorb_two_local || _absorb_two_trie) {
                    if(_absorb_two_trie && _absorb_two_local) {
                        // greedily pick the longer replacement
                        if(trie_->depth(p) > lce2) {
                            whence = AlgorithmCase::ABSORB_TWO_TRIE;
                        } else {
                            whence = AlgorithmCase::ABSORB_TWO_LOCAL;
                        }
                    } else if(_absorb_two_trie) {
                        whence = AlgorithmCase::ABSORB_TWO_TRIE;
                    } else {
                        whence = AlgorithmCase::ABSORB_TWO_LOCAL;
                    }
                } else {
                    auto const _absorb_one_local = absorb_one_local(m, len1, lce1);
                    auto const _absorb_one_trie = absorb_one_trie(m, p, len1);
                    if(_absorb_one_local || _absorb_one_trie) {
                        if(_absorb_one_local && _absorb_one_trie) {
                            // greedily pick the longer replacement
                            if(trie_->depth(p) > lce1) {
                                whence = AlgorithmCase::ABSORB_ONE_TRIE;
                            } else {
                                whence = AlgorithmCase::ABSORB_ONE_LOCAL;
                            }
                        } else if(_absorb_one_trie) {
                            whence = AlgorithmCase::ABSORB_ONE_TRIE;
                        } else {
                            whence = AlgorithmCase::ABSORB_ONE_LOCAL;
                        }
                    } else {
                        whence = AlgorithmCase::NEW_CHAR;
                    }
                }
            } else {
                precompute_absorb_local(m, len1, len2, windex, lce1, lnk1, lce2, lnk2);
                if(absorb_two_local(m, len1, len2, lce2)) {
                    whence = AlgorithmCase::ABSORB_TWO_LOCAL;
                } else if(absorb_one_local(m, len1, lce1)) {
                    whence = AlgorithmCase::ABSORB_ONE_LOCAL;
                } else {
                    whence = AlgorithmCase::NEW_CHAR;
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

            Index const border = window_begin_glob + curblock_window_offs;
            while(ztrie_ < z_ && ztrie_end_ + local_phrases_[ztrie_].len <= border) { // we go one phrase beyond the border according to [KK, 2017]
                // the phrase may be emitted
                if(on_emit_phrase) on_emit_phrase(local_phrases_[ztrie_]);

                // we enter phrases[ztrie]
                ztrie_end_ += local_phrases_[ztrie_].len;

                if constexpr(use_trie_) {
                    // count phrases that we introduced thanks to the trie
                    if(is_trie_ref(local_phrases_[ztrie_].link)) {
                        ++stats_.phrases_from_trie;
                        stats_.phrases_from_trie_total_len += local_phrases_[ztrie_].len;
                    }

                    // insert into trie
                    Index const rend = windex.pos_to_reverse(ztrie_end_ - window_begin_glob);
                    Index const rlen = windex.size() - 1 - rend;

                    trie_->insert(rfp, rend, rlen, max_block_);
                }

                // mark the phrase end for postprocessing of lnks
                windex.mark(ztrie_end_ - window_begin_glob, ztrie_, true);

                ++ztrie_;
            }
        }

        if(final_block) {
            // emit remaining phrases
            for(auto i = ztrie_; i <= z_; i++) {
                if(on_emit_phrase) on_emit_phrase(local_phrases_[i]);

                if constexpr(use_trie_) {
                    if(is_trie_ref(local_phrases_[i].link)) {
                        ++stats_.phrases_from_trie;
                        stats_.phrases_from_trie_total_len += local_phrases_[i].len;
                    }
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

template<bool use_trie, tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_lzend_compress(In begin, In const& end, Out out, size_t const max_block, size_t const k, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    // initialize encoding
    TopkHeader header(k, max_block, num_sketches, sketch_rows, sketch_columns);
    header.encode(out, MAGIC);

    BlockEncoder enc(out, block_size);
    setup_encoding(enc, k, use_trie);

    // init stats
    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;

    // initialize parser
    using Parser = LZEndTopkParser<use_trie>;
    using Trie = Parser::Trie;
    
    // parse
    Trie trie(k-1, num_sketches, sketch_rows, sketch_columns);
    Parser parser(max_block, trie);

    size_t i = 0;
    auto emit = [&](Parser::Phrase const& phrase) {
        if constexpr(PROTOCOL) {
            std::cout << "phrase #" << (num_phrases+1) << ": i=" << i << ", (";
            if(phrase.len > 1) {
                if(parser.is_trie_ref(phrase.link)) {
                    std::cout << (phrase.link - 3 * max_block) << "*, " << phrase.len;
                } else {
                    std::cout << phrase.link << ", " << phrase.len;
                }
            } else {
                std::cout << "0, 1";
            }
            std::cout << ", " << display(phrase.last) << ")" << std::endl;
        }
        
        i += phrase.len;

        ++num_phrases;
        if(phrase.len > 1) {
            // referencing phrase
            enc.write_uint(TOK_REF, phrase.link);
            enc.write_uint(TOK_LEN, phrase.len - 1);
            enc.write_char(TOK_LITERAL, phrase.last);

            ++num_ref;
        } else {
            // literal phrase
            ++num_literal;
            enc.write_uint(TOK_REF, 0);
            enc.write_char(TOK_LITERAL, phrase.last);
        }
        
        longest = std::max(longest, size_t(phrase.len));
        total_len += phrase.len;
        furthest = std::max(furthest, size_t(phrase.link));
        total_ref += phrase.link;
    };

    parser.on_emit_phrase = emit;
    parser.parse(begin, end);

    // flush writer
    enc.flush();

    // get parser stats
    auto const parser_stats = parser.stats();

    result.add("phrases_total", num_phrases);
    result.add("phrases_ref", num_ref);
    result.add("phrases_literal", num_literal);
    result.add("phrases_longest", longest);
    result.add("phrases_furthest", furthest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("phrases_avg_dist", std::round(100.0 * ((double)total_ref / (double)num_phrases)) / 100.0);
    result.add("phrases_from_trie", parser_stats.phrases_from_trie);
    result.add("phrases_from_trie_avg_len", std::round(100.0 * ((double)parser_stats.phrases_from_trie_total_len / (double)parser_stats.phrases_from_trie)) / 100.0);
}

template<bool use_trie, iopp::BitSource In, std::output_iterator<char> Out>
void topk_lzend_decompress(In in, Out out) {
    // header
    TopkHeader header(in, MAGIC);
    auto const k = header.k;
    auto const max_block = header.window_size;
    auto const num_sketches = header.num_sketches;
    auto const sketch_rows = header.sketch_rows;
    auto const sketch_columns = header.sketch_columns;

    auto const max_window = 3 * max_block;
    auto const trie_ref_offs = max_window;
    auto is_trie_ref = [&](Index const p){ return use_trie && p >= trie_ref_offs; };
    
    // initialize top-k framework
    using Trie = TopKLZEndTrie<Index>;
    Trie topk(k-1, num_sketches, sketch_rows, sketch_columns);

    // initialize buffers
    auto window = std::make_unique<char[]>(max_window);
    size_t wsize = 0; // current size of the window

    std::string rwindow;
    rwindow.reserve(max_window);
    auto phrase_buffer = std::make_unique<char[]>(max_window);

    auto local_phrases = std::make_unique<Index[]>(3 * max_window);
    Index z = 0;          // number of local phrases
    Index ztrie = 0;      // number of local phrases already in trie

    // decode
    size_t num_phrases = 0;
    size_t n = 0;     // number of decoded characters
    size_t phase = 0; // the current phase (a.k.a. "block")

    auto flush_trie = [&](Index const next_phase){
        if(next_phase < 2) return; // nothing to do yet

        Index const border = (next_phase >= 3) ? 2 * max_block : max_block;
        if constexpr(DEBUG) std::cout << "flushing trie preparing phase " << next_phase << " -> border=" << border << std::endl;

        auto beg = ztrie ? local_phrases[ztrie-1] + 1 : 0;
        if(ztrie >= z || beg >= border) {
            if constexpr(DEBUG) {
                std::cout << "nothing to flush";
                if(ztrie >= z) {
                    std::cout << " (no new phrases)";
                } else {
                    std::cout << " (latest non-trie phrase #" << ztrie << " begins at position " << beg << ")";
                }
                std::cout << std::endl;
            }
            return;
        }

        // for this, we first need to build a fingerprint index over the reverse window
        rwindow.clear();
        std::copy(window.get(), window.get() + wsize, std::back_inserter(rwindow));
        std::reverse(rwindow.begin(), rwindow.end());
        Trie::StringView rfp(rwindow);

        // now, we enter the relevant reverse strings beginning at the phrase end positions
        auto pos_to_reverse = [&](Index const pos){ return wsize - 1 - pos; };
        auto const rend = pos_to_reverse(0);

        while(ztrie < z && beg < border) {
            if constexpr(DEBUG) std::cout << "inserting string for phrase #" << (ztrie+1) << " (begins at " << beg << ", ends at " << local_phrases[ztrie] << ") into trie" << std::endl;
            auto const rpos = pos_to_reverse(local_phrases[ztrie]);
            topk.insert(rfp, rpos, rend - rpos + 1, max_block);

            beg = local_phrases[ztrie] + 1;
            ++ztrie;
        }

        if constexpr(DEBUG) {
            if(ztrie >= z) {
                std::cout << "(all phrases have been inserted)" << std::endl;
            } else {
                std::cout << "(next non-trie phrase #" << ztrie << " begins at position " << beg << " and is not inserted)" << std::endl;
            }
        }
    };

    auto slide_window = [&](Index const next_phase){
        if(next_phase < 3) return; // nothing to do, still filling up the window

        // slide window
        assert(wsize >= max_block);
        for(Index i = 0; i < 2 * max_block; i++) {
            window[i] = window[i + max_block];
        }
        wsize -= max_block;

        // slide local phrases
        Index discard = 0;
        while(discard < z && local_phrases[discard] < max_block) ++discard;

        if constexpr(DEBUG) std::cout << "discarding " << discard << " phrases that end before the window" << std::endl;

        assert(z >= discard);
        assert(ztrie >= discard);

        if(discard > 0) {
            for(Index x = 0; x + discard < z; x++) {
                assert(local_phrases[x + discard] >= max_block);

                if constexpr(DEBUG) std::cout << "\tdiscarding local phrase #" << x << " (ends at " << local_phrases[x] << ")" << std::endl;
                local_phrases[x] = local_phrases[x + discard] - max_block; // nb: also adjust end positions to new window
            }

            z -= discard;
            ztrie -= discard;
        }
    };

    auto prepare_phase = [&](Index const next_phase){
        if(next_phase > phase) {
            if constexpr(DEBUG) std::cout << "preparing next block: n=" << n << ", next_phase=" << next_phase << std::endl;

            assert(next_phase == phase + 1);
            if constexpr(use_trie) flush_trie(next_phase);
            slide_window(next_phase);

            phase = next_phase;
        }
    };

    auto emit = [&](char const c){
        if constexpr(DEBUG) std::cout << "\temit character (i=" << n << "): " << display(c) << std::endl;

        // emit character
        *out++ = c;
        
        window[wsize++] = c;
        assert(wsize <= max_window);

        ++n;
        if(in) prepare_phase(n / max_block);
    };

    BlockDecoder dec(in);
    setup_encoding(dec, k, use_trie);
    while(in) {
        auto const p = dec.read_uint(TOK_REF);
        auto const len = (p > 0) ? dec.read_uint(TOK_LEN) : 0;
        auto const c = dec.read_char(TOK_LITERAL);

        assert(len <= max_block);

        if constexpr(PROTOCOL) {
            std::cout << "phrase #" << (num_phrases+1) << ": i=" << n << ", (";
            if(p) {
                if(is_trie_ref(p)) {
                    std::cout << (p-trie_ref_offs) << "*, " << (len+1);
                } else {
                    std::cout << p << ", " << (len+1);
                }
            } else {
                std::cout << "0, 1";
            }
            std::cout << ", " << display(c) << ")" << std::endl;
        }

        if(p) {
            // referencing phrase

            // the phrase to decode may cross a block boundary
            // if it is a trie reference, it may then refer to a node that the encoder created while postprocessing the current block
            // if it is a local phrase reference, it may then refer to a phrase that lies in the first block AFTER the encoder slid the window
            // in any event, we would need to simulate the boundary crossing RIGHT NOW
            auto const next_phase = (n + len) / max_block;
            prepare_phase(next_phase);

            if(is_trie_ref(p)) {
                // spell trie string in reverse (which is forward again) and emit characters
                auto const d = topk.filter().spell_reverse(p - trie_ref_offs, phrase_buffer.get());
                assert(d >= len); // must have spelled at least len characters

                auto const beg = d - len;
                for(size_t i = 0; i < len; i++) {
                    emit(phrase_buffer[beg + i]);
                }
            } else {
                // copy characters from current window, because the calls to emit may proceed to the next window
                assert(local_phrases[p - 1] >= len - 1);
                auto const beg = local_phrases[p - 1] - len + 1;
                for(size_t i = 0; i < len; i++) {
                    phrase_buffer[i] = window[beg + i];
                }

                // now emit from buffer                
                for(size_t i = 0; i < len; i++) {
                    emit(phrase_buffer[i]);
                }
            }
        }

        // append literal
        emit(c);

        // count phrase
        if constexpr(DEBUG) std::cout << "-> ends at position " << (n - 1) << " (local " << wsize - 1 << ")" << std::endl;
        local_phrases[z++] = wsize - 1;
        if(z > 1) assert(local_phrases[z-1] - local_phrases[z-2] <= max_block);
        ++num_phrases;
    }
}
