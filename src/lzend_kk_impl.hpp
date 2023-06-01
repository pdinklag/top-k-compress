#include <cassert>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/util.hpp>
#include <alx_rmq.hpp>
#include <ankerl/unordered_dense.h>

#include <display.hpp>

#include <index/backward_search.hpp>
#include <index/btree.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

#include <lzend_parsing.hpp>
#include <lzend_rev_phrase_trie.hpp>

constexpr bool DEBUG = true;
constexpr bool PROTOCOL = true;

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'E') << 40 |
    ((uint64_t)'N') << 32 |
    ((uint64_t)'D') << 24 |
    ((uint64_t)'_') << 16 |
    ((uint64_t)'K') << 8 |
    ((uint64_t)'K');

using Index = uint32_t;

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void lzend_kk_compress(In begin, In const& end, Out out, size_t const max_block, size_t const block_size, pm::Result& result) {
    // init stats
    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;
    size_t max_consecutive_merges = 0;
    size_t num_consecutive_merges = 0;

    // initialize buffer for LZ-End parsing
    LZEndParsing<char, Index> phrases;

    std::vector<uint64_t> phrase_hashes;
    phrase_hashes.emplace_back(0); // phrase 0

    Index z = 0;
    Index ztrie = 1; // the first phrase that has not yet been entered into the trie

    // initialize the compact trie
    using Trie = LZEndRevPhraseTrie<char, Index>;
    using NodeNumber = Trie::NodeNumber;

    Trie trie(phrases);

    // initialize lnks and lens
    // at position j, lens contains the length of the phrase ending at j in the length-j prefix of the input text (a.k.a. the length of the current phrase)
    // lnks contains the number of the phrase FROM THE TRIE that ends at position j, or NIL if it either does not exist or lens[j] = 1
    constexpr Index NIL = 0;
    std::unique_ptr<Index[]> lnks = std::make_unique<Index[]>(3 * max_block);
    std::unique_ptr<Index[]> lens = std::make_unique<Index[]>(3 * max_block);
    for(size_t i = 0; i < 3 * max_block; i++) {
        lnks[i] = NIL;
        lens[i] = 0;
    }

    // prepare working memory
    std::string buffer(3 * max_block, 0); // contains the 3 most recent blocks

    std::string_view window;   // the entire current window of 3 blocks (or less at the beginning)

    // global LZEnd algorithm
    size_t phase = 0;
    while(begin != end) {
        if constexpr(DEBUG) {
            std::cout << std::endl;
            std::cout << "=== phase " << phase << " ===" << std::endl;
        }

        // slide previous two blocks
        for(size_t i = 0; i < 2 * max_block; i++) {
            buffer[i] = buffer[max_block + i];
            lens[i] = lens[max_block + i];
            lnks[i] = lnks[max_block + i];
        }

        // read next block
        Index const curblock_buffer_offs = 2 * max_block;
        Index curblock_size = 0;
        {
            while(begin != end && curblock_size < max_block) {
                buffer[curblock_buffer_offs + curblock_size] = *begin++;
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
                window = std::string_view(buffer.data() + 2 * max_block, curblock_size);
                window_begin_glob = 0;
                curblock_window_offs = 0;
                break;
            
            case 1:
                // in the second phase, we additionally deal with one previous block
                window = std::string_view(buffer.data() + max_block, max_block + curblock_size);
                window_begin_glob = 0;
                curblock_window_offs = max_block;
                break;
            
            default:
                // in subsequent phases, the window spans all 3 blocks
                window = std::string_view(buffer.data(), 2 * max_block + curblock_size);
                window_begin_glob = (phase - 2) * max_block;
                curblock_window_offs = 2 * max_block;
                break;
        }

        if constexpr(DEBUG) {
            std::cout << "curblock_size=" << curblock_size << ", curblock_window_offs=" << curblock_window_offs << ", window.size()=" << window.size() << std::endl;
            std::cout << "computing index..." << std::endl;
        }
        
        // compute inverse suffix array and LCP array of reverse window
        std::unique_ptr<uint32_t[]> lcp;
        std::unique_ptr<uint32_t[]> isa;
        std::string rwindow;
        {
            // reverse window
            rwindow.reserve(window.size()+1);
            std::copy(window.rbegin(), window.rend(), std::back_inserter(rwindow));
            rwindow.push_back(0); // make sure that the last suffix is the lexicographically smallest

            // compute inverse suffix array and LCP array of reverse window
            auto [_sa, _isa, _lcp] = tdc::text::sa_isa_lcp_u32(rwindow.begin(), rwindow.end());
            assert(_sa[0] == window.size());

            // keep inverse suffix array and LCP array, discard suffix array and reversed window
            lcp = std::move(_lcp);
            isa = std::move(_isa);
        }

        using FPString = FPStringView<char>;
        FPString rwindow_fp(rwindow);

        // translate a position in the block to the corresponding position in the reverse block (which has a sentinel!)
        auto pos_to_reverse = [&](Index const i) { return window.size() - (i+1); };

        // compute rmq on LCP array
        alx::rmq::rmq_n rmq(lcp.get(), window.size() + 1);
        // RMQRMM64 rmq((std::make_signed_t<Index>*)lcp.get(), window.size()+1); // argh... the RMQ constructor wants signed integers

        // initialize "marked binary tree" (a.k.a. predessor + successor data structure)
        // position j is marked iff a phrase ends at position SA[j]
        // -> M contains suffix array positions
        // nb: the "balanced tree" P is also simulated by this data structure,
        //     which we modified to mark positions along with the corresponding phrase number
        //     this is also what avoids the need to keep the suffix array in RAM
        struct MarkedLCP {
            Index sa_pos;     // the position in the suffix array of the text position at which the phrase ends
            Index phrase_num; // the phrase number

            // std::totally_ordered
            bool operator==(MarkedLCP const& x) const { return sa_pos == x.sa_pos; }
            bool operator!=(MarkedLCP const& x) const { return sa_pos != x.sa_pos; }
            bool operator< (MarkedLCP const& x) const { return sa_pos <  x.sa_pos; }
            bool operator<=(MarkedLCP const& x) const { return sa_pos <= x.sa_pos; }
            bool operator> (MarkedLCP const& x) const { return sa_pos >  x.sa_pos; }
            bool operator>=(MarkedLCP const& x) const { return sa_pos >= x.sa_pos; }
        } __attribute__((__packed__));

        constexpr Index DONTCARE = 0; // used for querying M for a certain suffix array position (key); the phrase number then doesn't matter

        BTree<MarkedLCP, 65> marked;
        using MResult = decltype(marked)::KeyResult;

        #ifndef NDEBUG
        auto is_marked = [&](Index const m) {
            auto const isa_m = isa[pos_to_reverse(m)];
            return marked.contains(MarkedLCP{isa_m, DONTCARE});
        };
        #endif

        auto mark = [&](Index const m, Index const phrase_num, bool silent = false){
            // register that phrase phrase_num ends at text position pos
            if constexpr(DEBUG) {
                auto const mglob = window_begin_glob + m;
                if(!silent) std::cout << "\tregister phrase " << phrase_num << " ending at " << mglob << std::endl;
            }
            #ifndef NDEBUG
            assert(!is_marked(m));
            #endif
            auto const isa_m = isa[pos_to_reverse(m)];
            marked.insert(MarkedLCP{isa_m, phrase_num});
        };

        auto unmark = [&](Index const m, bool silent = false){
            // unregister phrase that ends at text position m
            if constexpr(DEBUG) {
                auto const mglob = window_begin_glob + m;
                if(!silent) std::cout << "\tunregister phrase ending at " << mglob << std::endl;
            }
            auto const isa_m = isa[pos_to_reverse(m)];
            assert(marked.contains(MarkedLCP{isa_m, DONTCARE}));
            marked.remove(MarkedLCP{isa_m, DONTCARE});
        };

        auto marked_lcp = [&](Index const q) {
            auto const isa_q = isa[pos_to_reverse(q)];

            // look for the marked LCPs sorrounding the suffix array position of q
            auto const marked_l = (isa_q > 0) ? marked.predecessor(MarkedLCP{isa_q - 1, DONTCARE}) : MResult{ false, 0 };
            auto const lce_l = marked_l.exists ? lcp[rmq.rmq(marked_l.key.sa_pos + 1, isa_q)] : 0;
            auto const marked_r = marked.successor(MarkedLCP{isa_q + 1, DONTCARE});
            auto const lce_r = marked_r.exists ? lcp[rmq.rmq(isa_q + 1, marked_r.key.sa_pos)] : 0;

            // select the longer LCP and return it along with the corresponding phrase number
            return (lce_l > lce_r) ? std::pair(lce_l, marked_l.key.phrase_num) : std::pair(lce_r, marked_r.key.phrase_num);
        };

        // preprocess: mark positions of phrases that end in the previous two blocks within the window
        {
            if constexpr(DEBUG) {
                std::cout << "preprocessing next block..." << std::endl;
            }

            Index x = z;
            while(x > 0 && phrases[x].end >= window_begin_glob) {
                assert(phrases[x].end < phase * max_block); // previously computed phrases cannot end in the new block that we just read from the input
                mark(phrases[x].end - window_begin_glob, x, true);
                --x;
            }
        }

        // begin LZEnd algorithm by [Kempa & Kosolobov, 2017]
        for(Index mblock = 0; mblock < curblock_size; mblock++) {
            auto const m = curblock_window_offs + mblock;  // the current position within the window
            auto const mglob = window_begin_glob + m; // the current global position in the input

            if constexpr(DEBUG) {
                std::cout << std::endl;
                std::cout << "--- mblock=" << mblock << " -> mglob=" << mglob << " ---" << std::endl;
                std::cout << "next character: " << display(window[mblock]) << std::endl;
            }

            Index p = 0;
            if(phase >= 2) {
                auto const rsuf_begin = pos_to_reverse(m-1);
                auto const rsuf_len = rwindow.size() - 1 - rsuf_begin;
                p = trie.approx_find_phr(rwindow_fp, rsuf_begin, rsuf_len);
            }

            lnks[m] = NIL;
            auto const len1 = phrases[z].len;                        // length of the current phrase
            auto const len2 = len1 + (z > 0 ? phrases[z-1].len : 0); // total length of the two current phrases

            // sanity
            #ifndef NDEBUG
            if(m > 0) assert(is_marked(m-1));
            if(m > len1 && z > 1) assert(is_marked(m-1-len1));
            #endif

            auto commonPart = [&](Index const len){
                auto const plen = phrases[p].len;
                if(p > 0 && m > plen) {
                    if constexpr(DEBUG) {
                        std::cout << "\ttesting whether trie phrases " << p << " and " << (p-1) << " have a common suffix of length " << len << " with current input suffix" << std::endl;
                    }
                    assert(plen > 0);

                    if(plen < len) {
                        auto const pos = m - plen;
                        auto const lhash = rwindow_fp.fingerprint(pos_to_reverse(m-1), pos_to_reverse(pos));
                        if(phrase_hashes[p] == lhash) {
                            if(lens[pos] - 1 + plen == len) {
                                if(lnks[pos] != NIL) {
                                    auto const nca_len = trie.nca_len(lnks[pos], p-1);
                                    if(nca_len + plen >= len) {
                                        std::cout << "\t\tTRUE - combined length of NCA and phrase matches" << std::endl;
                                        return true;
                                    } else {
                                        std::cout << "\t\tFALSE - combined length of NCA and phrase do not match" << std::endl;
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
                                    std::cout << "\t\tFALSE - combined length of trie phrase " << p << " (" << plen << ") and lens[" << posglob << "]=" << lens[pos] << " minus 1 does not match" << std::endl;
                                }
                            }
                        } else {
                            if constexpr(DEBUG) {
                                std::cout << "\t\tFALSE - trie phrase hash (0x" << std::hex << phrase_hashes[p] << " does not match current suffix hash (0x" << lhash << std::dec << ")" << std::endl;
                            }
                        }
                    } else {
                        if constexpr(DEBUG) {
                            std::cout << "\t\tFALSE - trie phrase " << p << " is already too long by itself" << std::endl;
                        }
                    }
                }
                return false;
            };

            auto absorbOne = [&](){
                if(p > 0 && m > 0) {
                    if constexpr(DEBUG) {
                        std::cout << "\ttesting whether trie phrase " << p << " has a common suffix with current phrase of length " << len1 << std::endl;
                    }
                    auto const plen = phrases[p].len;
                    if(plen < len1) {
                        std::cout << "\t\ttrie phrase " << p << " is shorter than current phrase, delegating" << std::endl;
                        if(commonPart(len1)) {
                            if constexpr(DEBUG) {
                                std::cout << "\tabsorbOne returned true" << std::endl;
                            }
                            return true;
                        }
                    } else {
                        if(phrases[p].last == phrases[z].last) {
                            if(len1 > 1 && lnks[m-1] == NIL) {
                                if constexpr(DEBUG) {
                                    std::cout << "\t\tFALSE - a phrase ends at the previous position, but the trie link is NIL" << std::endl;
                                }
                            } else {
                                if(len1 == 1) {
                                    std::cout << "\t\tTRUE - last character matches for length-1 phrase" << std::endl;
                                    std::cout << "\tabsorbOne returned true" << std::endl;
                                    return true;
                                } else {
                                    auto const nca_len = trie.nca_len(lnks[m-1], phrases[p].link);
                                    if(nca_len + 1 >= len1) {
                                        std::cout << "\t\tTRUE - NCA length plus 1 exceeds current phrase length" << std::endl;
                                        std::cout << "\tabsorbOne returned true" << std::endl;
                                        return true;
                                    } else  {
                                        std::cout << "\t\tFALSE - the NCA length plus 1 is too short" << std::endl;
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
            };

            // query the marked LCP data structure for the previous position and compute LCE
            auto absorbOne2 = [&](Index& out_lnk){
                if(m > 0 && len1 < window.size()) {
                    Index lce;
                    std::tie(lce, out_lnk) = marked_lcp(m - 1);

                    if constexpr(DEBUG) {
                        if(lce >= len1) std::cout << "\tabsorbOne2 returned true" << std::endl;
                    }
                    return lce >= len1;
                }
                return false;
            };

            // query the trie for a suffix matching the two current phrases
            auto absorbTwo = [&](){
                if(commonPart(len2)) {
                    if constexpr(DEBUG) {
                        std::cout << "\tabsorbTwo returned true" << std::endl;
                    }
                    return true;
                } else {
                    return false;
                }
            };

            // query the marked LCP data structure for the previous position, excluding the most current phrase, and compute LCE
            auto absorbTwo2 = [&](Index& out_lnk){
                if(m > len1 && len2 < window.size()) {
                    // TODO: the temporary unmarking/marking isn't really nice, but it does simply the code a lot
                    // it can be avoided by expanding the implementation of marked_lcp and only doing two offset predecessor and successor queries
                    // it is, however, not really clear how much of a performance impact this has

                    // we exclude the end position of the previous phrase by temporarily unmarking it
                    unmark(m-1-len1, true);

                    // 
                    Index lce;
                    std::tie(lce, out_lnk) = marked_lcp(m - 1);

                    // now, we mark it again
                    mark(m-1-len1, z-1, true);

                    if constexpr(DEBUG) {
                        if(lce >= len2) std::cout << "\tabsorbTwo2 returned true" << std::endl;
                    }
                    return lce >= len2;
                }
                return false;
            };

            char const next_char = window[m];

            Index ptr;
            bool localTwo = false;
            bool localOne = false;
            if(absorbTwo() || (localTwo = absorbTwo2(ptr))) {
                // merge the two current phrases and extend their length by one
                if constexpr(DEBUG) std::cout << "\tMERGE phrases " << z << " and " << z-1 << " to new phrase of length " << (len2+1) << std::endl;

                // updateRecent: unregister current phrase
                unmark(m - 1);

                // updateRecent: unregister previous phrase
                unmark(m - 1 - len1);

                // delete current phrase
                phrases.pop_back();
                phrase_hashes.pop_back();
                --z;
                assert(z); // nb: must still have at least phrase 0

                if(localTwo) {
                    // we are here because of absorbTwo2 (local index)
                    p = ptr;
                } else {
                    // we are here because of absorbTwo (trie)
                    lnks[m] = p;
                    if constexpr(DEBUG) std::cout << "\tsetting lnks[" << mglob << "] := " << p << std::endl;
                }

                // merge phrases
                phrases.replace_back(p, len2 + 1, next_char);

                // stats
                ++num_consecutive_merges;
                max_consecutive_merges = std::max(max_consecutive_merges, num_consecutive_merges);
            } else if(absorbOne() || (localOne = absorbOne2(ptr))) {
                // extend the current phrase by one character
                if constexpr(DEBUG) std::cout << "\tEXTEND phrase " << z << " to length " << (len1+1) << std::endl;

                // updateRecent: unregister current phrase
                unmark(m - 1);

                if(localOne) {
                    // we are here because of absorbTwo2 (local index)
                    p = ptr;
                } else {
                    // we are here because of absorbTwo (trie)
                    lnks[m] = p;
                    if constexpr(DEBUG) std::cout << "\tsetting lnks[" << mglob << "] := " << p << std::endl;
                }

                // extend phrase
                phrases.replace_back(p, len1 + 1, next_char);

                // stats
                num_consecutive_merges = 0;
            } else {
                // begin a new phrase of initially length one
                if constexpr(DEBUG) std::cout << "\tNEW phrase " << (z+1) << " of length 1" << std::endl;
                
                ++z;
                phrases.emplace_back(p, 1, next_char);
                phrase_hashes.emplace_back(0);

                // stats
                num_consecutive_merges = 0;
            }

            if constexpr(DEBUG) std::cout << "\t-> z=" << z << ", link=" << phrases[z].link << ", len=" << phrases[z].len << ", last=" << display(phrases[z].last) << std::endl;

            // update lens
            lens[m] = phrases[z].len;

            // update phrase hash
            phrase_hashes[z] = rwindow_fp.fingerprint(pos_to_reverse(m), pos_to_reverse(m-phrases[z].len+1));

            // updateRecent: register updated current phrase
            mark(m, z);
            assert(is_marked(m));
        }

        if(phase >= 1 && begin != end) {
            if constexpr(DEBUG) {
                std::cout << std::endl;
                std::cout << "postprocessing ..." << std::endl;
            }

            // insert phrases that end in the first two blocks within the window into the trie
            // while doing that, also recompute marked to contain only phrases that are inserted
            marked.clear();

            if constexpr(DEBUG) {
                std::cout << "inserting phrases ending in sliding block into trie ..." << std::endl;
            }

            Index const border = window_begin_glob + curblock_window_offs;
            while(ztrie < z && phrases[ztrie].end <= border) { // we go one phrase beyond the border according to [KK, 2017]
                // insert phrases[ztrie]
                Index const rend = pos_to_reverse(phrases[ztrie].end - window_begin_glob);
                Index const len = phrases[ztrie].len;
                assert(rwindow[rend] == phrases[ztrie].last); // sanity check
                assert(len < window.size()); // Lemma 9 of [KK, 2017] implies this

                trie.insert(rwindow_fp, rend, rwindow.size() - 1 - rend);
                mark(phrases[ztrie].end - window_begin_glob, ztrie, true);

                ++ztrie;
            }

            // update lnks
            if constexpr(DEBUG) {
                std::cout << "postprocessing lnks ..." << std::endl;
            }
            for(Index i = 0; i < curblock_size; i++) {
                auto const q = curblock_window_offs + i;  // the current position within the window

                // update if necessary
                if(lens[q] > 1 && lnks[q] == NIL) { 
                    auto [ln, x] = marked_lcp(q-1);
                    if(lens[q] <= ln + 1) { // nb: +1 because the phrase length includes the final as well -- this appears to be a mistake in [KK, 2017]
                        assert(x <= ztrie); // we cannot refer to a phrase that is not yet in the trie
                        lnks[q] = x;

                        if constexpr(DEBUG) {
                            auto const qglob = window_begin_glob + q;
                            std::cout << "\tsetting lnks[" << qglob << "] := " << x << std::endl;
                        }
                        // nb: lens isn't updated apparently -- at the time of writing, I'd lie if I knew why
                    } else {
                        if constexpr(DEBUG) {
                            auto const qglob = window_begin_glob + q;
                            std::cout << "\tleaving lnks[" << qglob << "] at NIL because ln=" << ln << " (with phrase x=" << (ln > 0 ? x : 0) << ") plus 1 is less than lens[" << qglob << "]=" << lens[q] << std::endl;
                        }
                    }
                } else {
                    if constexpr(DEBUG) {
                        auto const qglob = window_begin_glob + q;
                        std::cout << "\tleaving lnks[" << qglob << "] = " << lnks[q] << ", lens[" << qglob << "] = " << lens[q] << std::endl;
                    }
                }
            }
        }

        // advance to next phase
        ++phase;
    }

    // initialize encoding
    out.write(MAGIC, 64);
    PhraseBlockWriter writer(out, block_size, true);

    // write phrases
    {
        if constexpr(PROTOCOL && DEBUG) {
            std::cout << std::endl;
            std::cout << "phrase protocol:" << std::endl;
        }

        size_t i = 0;
        for(size_t j = 1; j <= z; j++) {
            if constexpr(PROTOCOL) {
                std::cout << "phrase #" << j << ": i=" << i <<
                    ", (" << phrases[j].link << ", " << phrases[j].len << ", " << display(phrases[j].last) <<
                    "), hash=0x" << std::hex << phrase_hashes[j] << std::dec << std::endl;
            }
            
            i += phrases[j].len;

            ++num_phrases;
            if(phrases[j].len > 1) {
                // referencing phrase
                writer.write_ref(phrases[j].link);
                writer.write_len(phrases[j].len - 1);
                writer.write_literal(phrases[j].last);

                ++num_ref;

            } else {
                // literal phrase
                ++num_literal;
                writer.write_ref(0);
                writer.write_literal(phrases[j].last);
            }
            
            longest = std::max(longest, size_t(phrases[j].len));
            total_len += phrases[j].len;
            furthest = std::max(furthest, size_t(phrases[j].link));
            total_ref += phrases[j].link;
        }

        if constexpr(PROTOCOL) std::cout << std::endl;
    }

    // flush
    writer.flush();
    
    // get stats
    result.add("phrases_total", num_phrases);
    result.add("phrases_ref", num_ref);
    result.add("phrases_literal", num_literal);
    result.add("phrases_longest", longest);
    result.add("phrases_furthest", furthest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("phrases_avg_dist", std::round(100.0 * ((double)total_ref / (double)num_phrases)) / 100.0);
    result.add("max_consecutive_merges", max_consecutive_merges);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void lzend_kk_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }
    
    std::string dec;
    std::vector<size_t> factors;
    
    PhraseBlockReader reader(in, true);
    while(in) {
        auto const q = reader.read_ref();
        auto const len = (q > 0) ? reader.read_len() : 0;

        if(len > 0) {
            auto p = factors[q-1] + 1 - len;
            for(size_t i = 0; i < len; i++) {
                dec.push_back(dec[p++]);
            }
        }
        
        if(in) {
            auto const c = reader.read_literal();
            factors.push_back(dec.length());
            dec.push_back(c);

            if constexpr(PROTOCOL) {
                std::cout << "factor #" << factors.size() << ": i=" << (dec.size() - len - 1) << ", (" << q << ", " << len << ", " << display(c) << ")" << std::endl;
            }
        } else {
            if constexpr(PROTOCOL) {
                std::cout << "factor #" << factors.size() << ": i=" << (dec.size() - len - 1) << ", (" << q << ", " << len << ", <EOF>)" << std::endl;
            }
        }
    }

    // output
    std::copy(dec.begin(), dec.end(), out);
}
