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

constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;
constexpr bool PARANOID = true;

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
        //     which we modified to mark positions along with the corresponding phrase number (values)
        //     this is also what avoids the need to keep the suffix array in RAM
        BTree<Index, Index, 65> marked;
        using MResult = KeyValueResult<Index, Index>;

        #ifndef NDEBUG
        auto is_marked = [&](Index const m) {
            auto const isa_m = isa[pos_to_reverse(m)];
            return marked.contains(isa_m);
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
            marked.insert(isa_m, phrase_num);
        };

        auto unmark = [&](Index const m, bool silent = false){
            // unregister phrase that ends at text position m
            if constexpr(DEBUG) {
                auto const mglob = window_begin_glob + m;
                if(!silent) std::cout << "\tunregister phrase ending at " << mglob << std::endl;
            }
            auto const isa_m = isa[pos_to_reverse(m)];
            assert(marked.contains(isa_m));
            marked.remove(isa_m);
        };

        auto marked_lcp = [&](Index const q) {
            auto const isa_q = isa[pos_to_reverse(q)];

            // look for the marked LCPs sorrounding the suffix array position of q
            auto const marked_l = (isa_q > 0) ? marked.predecessor(isa_q - 1) : MResult::none();
            auto const lce_l = marked_l.exists ? lcp[rmq.rmq(marked_l.key + 1, isa_q)] : 0;
            auto const marked_r = marked.successor(isa_q + 1);
            auto const lce_r = marked_r.exists ? lcp[rmq.rmq(isa_q + 1, marked_r.key)] : 0;

            // select the longer LCP and return it along with the corresponding phrase number
            return (lce_l > lce_r) ? std::pair(lce_l, marked_l.value) : std::pair(lce_r, marked_r.value);
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

                Index hash_match;
                p = trie.approx_find_phr(rwindow_fp, rsuf_begin, rsuf_len, hash_match);

                #ifndef NDEBUG
                if constexpr(PARANOID) {
                    if(p > 0 && hash_match > 0) {
                        std::string rsuf;
                        rsuf.reserve(hash_match);
                        phrases.extract_reverse_phrase_suffix(std::back_inserter(rsuf), p, hash_match);
                        
                        auto const* rstr = rwindow_fp.data() + rsuf_begin;
                        for(size_t i = 0; i < hash_match; i++) {
                            auto const actual = rstr[i];
                            auto const expect = rsuf[i];
                            assert(actual == expect);
                        }
                    }
                }
                #endif
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
                if(p > 0 && m > 0 && len1 < max_block) {
                    if constexpr(DEBUG) {
                        std::cout << "\ttesting whether trie phrase " << p << " has a common suffix with current phrase of length " << len1 << std::endl;
                    }
                    auto const plen = phrases[p].len;
                    if(plen < len1) {
                        if constexpr(DEBUG) {
                            std::cout << "\t\ttrie phrase " << p << " is shorter than current phrase, delegating" << std::endl;
                        }
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
                                    if constexpr(DEBUG) {
                                        std::cout << "\t\tTRUE - last character matches for length-1 phrase" << std::endl;
                                        std::cout << "\tabsorbOne returned true" << std::endl;
                                    }
                                    return true;
                                } else {
                                    auto const nca_len = trie.nca_len(lnks[m-1], phrases[p].link);
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
            };

            // query the trie for a suffix matching the two current phrases
            auto absorbTwo = [&](){
                if(len2 < max_block && commonPart(len2)) {
                    if constexpr(DEBUG) {
                        std::cout << "\tabsorbTwo returned true" << std::endl;
                    }
                    return true;
                } else {
                    return false;
                }
            };

            // combined precomputation of absorbOne2 and absorbTwo2
            // even though absorbOne2 is never needed if absorbTwo2 returns true,
            // the result of absorbOne2 can be used to compute absorbTwo2 without the need for temporarily unmarking and re-marking a phrase
            // furthermore, the number of predecessor and successor queries is minimized this way
            Index lce1 = 0, lnk1 = 0; // corresponds to absorbOne2
            Index lce2 = 0, lnk2 = 0; // corresponds to absorbTwo2
            auto precompute_absorb2 = [&](){
                if(m > 0 && len1 < window.size()) {
                    auto const isa_cur = isa[pos_to_reverse(m-1)];
                    
                    // this is basically marked_lcp for m-1, except we want to keep all intermediate results for further computation of absorbTwo2
                    auto const marked_l1 = (isa_cur > 0) ? marked.predecessor(isa_cur - 1) : MResult::none();
                    auto const lce_l1 = marked_l1.exists ? lcp[rmq.rmq(marked_l1.key + 1, isa_cur)] : 0;
                    auto const marked_r1 = marked.successor(isa_cur + 1);
                    auto const lce_r1 = marked_r1.exists ? lcp[rmq.rmq(isa_cur + 1, marked_r1.key)] : 0;

                    if(lce_l1 > 0 || lce_r1 > 0) {
                        // find marked position with larger LCE
                        if(lce_l1 > lce_r1) {
                            lnk1 = marked_l1.value;
                            lce1 = lce_l1;
                        } else {
                            lnk1 = marked_r1.value;
                            lce1 = lce_r1;
                        }

                        // additionally, perform queries excluding the end position of the previous phrase
                        if(m > len1 && len2 < window.size()) {
                            auto const exclude = z - 1;

                            auto marked_l2 = marked_l1;
                            auto lce_l2 = lce_l1;
                            if(marked_l2.exists && marked_l2.value == exclude) {
                                // ignore end position of previous phrase
                                marked_l2 = (marked_l1.key > 0 ? marked.predecessor(marked_l1.key - 1) : MResult::none());
                                lce_l2 = marked_l2.exists ? lcp[rmq.rmq(marked_l2.key + 1, isa_cur)] : 0;
                            }

                            auto marked_r2 = marked_r1;
                            auto lce_r2 = lce_r1;
                            if(marked_r2.exists && marked_r2.value == exclude) {
                                // ignore end position of previous phrase
                                marked_r2 = marked.successor(marked_r1.key + 1);
                                lce_r2 = marked_r2.exists ? lcp[rmq.rmq(isa_cur + 1, marked_r2.key)] : 0;
                            }

                            // find marked position with larger LCE
                            if(lce_l2 > 0 || lce_r2 > 0) {
                                if(lce_l2 > lce_r2) {
                                    lnk2 = marked_l2.value;
                                    lce2 = lce_l2;
                                } else {
                                    lnk2 = marked_r2.value;
                                    lce2 = lce_r2;
                                }
                            }
                        }
                    }
                }
            };

            // query the marked LCP data structure for the previous position, excluding the most current phrase, and compute LCE
            auto absorbTwo2 = [&](Index& out_lnk){
                precompute_absorb2();
                if(m > len1 && len2 < max_block && lce2 >= len2) {
                    out_lnk = lnk2;
                    return true;
                } else {
                    return false;
                }
            };

            // query the marked LCP data structure for the previous position and compute LCE
            auto absorbOne2 = [&](Index& out_lnk){
                // we expect that precompute_absorb2 has already been called
                if(m > 0 && len1 < max_block && lce1 >= len1) {
                    out_lnk = lnk1;
                    return true;
                } else {
                    return false;
                }
            };

            char const next_char = window[m];

            Index ptr;
            bool localTwo = false;
            bool localOne = false;

            #ifndef NDEBUG
            enum AlgorithmCase {
                NONE,
                ABSORB_ONE,
                ABSORB_ONE2,
                ABSORB_TWO,
                ABSORB_TWO2,
                NEW_CHAR
            };
            AlgorithmCase whence = AlgorithmCase::NONE;
            #endif

            if(absorbTwo() || (localTwo = absorbTwo2(ptr))) {
                #ifndef NDEBUG
                whence = localTwo ? AlgorithmCase::ABSORB_TWO2 : AlgorithmCase::ABSORB_TWO;
                #endif

                // merge the two current phrases and extend their length by one
                if constexpr(DEBUG) std::cout << "\tMERGE phrases " << z << " and " << z-1 << " to new phrase of length " << (len2+1) << std::endl;

                // updateRecent: unregister current phrase
                unmark(m - 1);

                // updateRecent: unregister previous phrase
                unmark(m - 1 - len1);

                // delete current phrase
                phrases.pop_back<PARANOID>();
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
                phrases.replace_back<PARANOID>(p, len2 + 1, next_char);

                // stats
                ++num_consecutive_merges;
                max_consecutive_merges = std::max(max_consecutive_merges, num_consecutive_merges);
            } else if(absorbOne() || (localOne = absorbOne2(ptr))) {
                #ifndef NDEBUG
                whence = localOne ? AlgorithmCase::ABSORB_ONE2 : AlgorithmCase::ABSORB_ONE;
                #endif

                // extend the current phrase by one character
                if constexpr(DEBUG) std::cout << "\tEXTEND phrase " << z << " to length " << (len1+1) << std::endl;

                // updateRecent: unregister current phrase
                unmark(m - 1);

                if(localOne) {
                    // we are here because of absorbOne2 (local index)
                    p = ptr;
                } else {
                    // we are here because of absorbOne (trie)
                    lnks[m] = p;
                    if constexpr(DEBUG) std::cout << "\tsetting lnks[" << mglob << "] := " << p << std::endl;
                }

                // extend phrase
                phrases.replace_back<PARANOID>(p, len1 + 1, next_char);

                // stats
                num_consecutive_merges = 0;
            } else {
                #ifndef NDEBUG
                whence = AlgorithmCase::NEW_CHAR;
                #endif

                // begin a new phrase of initially length one
                if constexpr(DEBUG) std::cout << "\tNEW phrase " << (z+1) << " of length 1" << std::endl;
                
                ++z;
                phrases.emplace_back<PARANOID>(p, 1, next_char);
                phrase_hashes.emplace_back(0);

                // stats
                num_consecutive_merges = 0;
            }

            if constexpr(DEBUG) std::cout << "\t-> z=" << z << ", link=" << phrases[z].link << ", len=" << phrases[z].len << ", last=" << display(phrases[z].last) << std::endl;

            // update lens
            assert(phrases[z].len <= max_block);
            lens[m] = phrases[z].len;

            #ifndef NDEBUG
            if constexpr(PARANOID) {
                // verify that the current phrase is a valid LZEnd phrase
                if(phrases[z].len > 1) {
                    auto const lnk = phrases[z].link;
                    assert(lnk > 0);
                    auto const len = phrases[z].len - 1;

                    std::string rsuf;
                    rsuf.reserve(len);
                    phrases.extract_reverse_phrase_suffix(std::back_inserter(rsuf), lnk, len);
                    
                    auto const offs = pos_to_reverse(m-1);
                    auto const* rstr = rwindow_fp.data() + offs;
                    for(size_t i = 0; i < len; i++) {
                        auto const actual = rstr[i];
                        auto const expect = rsuf[i];
                        assert(actual == expect);
                    }
                }
            }
            #endif

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
                // we enter phrases[ztrie]

                // enter the phrase into the parsing's successor data structure
                // once a phrase is in the trie, we may want to decode it in order to do longest common suffix queries
                // before that, it is never necessary to decode it, and thus it suffices to update the successor data structure now
                if constexpr(PARANOID) {
                    // we persist phrases immediately
                } else {
                    phrases.persist(ztrie);
                }

                // insert into trie
                Index const rend = pos_to_reverse(phrases[ztrie].end - window_begin_glob);
                Index const rlen = rwindow.size() - 1 - rend;
                Index const len = phrases[ztrie].len;
                assert(rwindow[rend] == phrases[ztrie].last); // sanity check
                assert(len < window.size()); // Lemma 9 of [KK, 2017] implies this

                trie.insert(rwindow_fp, rend, rlen);

                // mark the phrase end for postprocessing of lnks
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
