#include <cassert>
#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/util.hpp>
#include <RMQRMM64.h>
#include <ankerl/unordered_dense.h>

#include <display.hpp>

#include <index/backward_search.hpp>
#include <index/btree.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;

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
void lzend_kk_compress(In begin, In const& end, Out out, size_t const max_window, size_t const block_size, pm::Result& result) {
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

    // initialize phrase buffer
    struct Phrase {
        char     c;
        Index    len;
        uint64_t hash;
        Index    lnk;
    };

    std::vector<Phrase> phrases;
    phrases.push_back({0,0,0,0}); // ensure that there is a phrase 0

    Index z = 0;

    // prepare working memory
    std::string window;
    window.reserve(max_window);

    // global LZEnd algorithm
    size_t step = 0;
    while(begin != end) {
        // read next window into RAM
        window.clear();
        while(begin != end && window.size() < max_window) {
            window.push_back(*begin++);
        }
        
        Index const window_size = window.size();

        // compute inverse suffix array and LCP array of reverse window
        std::unique_ptr<uint32_t[]> lcp;
        std::unique_ptr<uint32_t[]> isa;
        {
            // reverse window
            std::string r;
            r.reserve(window_size+1);
            std::copy(window.rbegin(), window.rend(), std::back_inserter(r));
            r.push_back(0); // make sure that the last suffix is the lexicographically smallest

            // compute inverse suffix array and LCP array of reverse window
            auto [_sa, _isa, _lcp] = tdc::text::sa_isa_lcp_u32(r.begin(), r.end());
            assert(_sa[0] == window_size);

            // keep inverse suffix array and LCP array, discard suffix array and reversed window
            lcp = std::move(_lcp);
            isa = std::move(_isa);
        }

        // translate a position in the text to the corresponding position in the reverse text (which has a sentinel!)
        auto pos_to_reverse = [&](Index const i) { return window_size - (i+1); };

        // compute rmq on LCP array
        RMQRMM64 rmq((std::make_signed_t<Index>*)lcp.get(), window_size+1); // argh... the RMQ constructor wants signed integers

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

        BTree<MarkedLCP, 65> marked;
        using MResult = decltype(marked)::KeyResult;

        auto mark = [&](Index const pos, Index const phrase_num){
            // register that phrase phrase_num ends at text position pos
            if constexpr(DEBUG) std::cout << "\tregister phrase " << phrase_num << " ending at " << pos << std::endl;
            auto const isa_m = isa[pos_to_reverse(pos)];
            marked.insert(MarkedLCP{isa_m, phrase_num});
        };

        auto unmark = [&](Index const m){
            // unregister phrase that ends at text position m
            if constexpr(DEBUG) std::cout << "\tunregister phrase ending at " << m << std::endl;
            auto const isa_m = isa[pos_to_reverse(m)];
            assert(marked.contains(MarkedLCP{isa_m, 0}));
            marked.remove(MarkedLCP{isa_m, 0});
        };

        #ifndef NDEBUG
        auto is_marked = [&](Index const m) {
            auto const isa_m = isa[pos_to_reverse(m)];
            return marked.contains(MarkedLCP{isa_m, 0});
        };
        #endif

        // begin LZEnd algorithm by [Kempa & Kosolobov, 2017]
        for(Index m = 0; m < window_size; m++) {
            auto const mglob = step * max_window + m;
            if constexpr(DEBUG) std::cout << "mglob=" << mglob << ", m=" << m << ", window[m]=" << display(window[m]) << std::endl;

            Index p = 0;
            auto const len1 = phrases[z].len;                        // length of the current phrase
            auto const len2 = len1 + (z > 0 ? phrases[z-1].len : 0); // total length of the two current phrases

            // sanity
            #ifndef NDEBUG
            if(m > 0) assert(is_marked(m-1));
            if(m > len1 && z > 1) assert(is_marked(m-1-len1));
            #endif

            // query the marked LCP data structure for the previous position and compute LCEs
            Index lce1 = 0;
            Index lnk1 = 0;

            // additionally, excluding the end position of the previous phrase
            Index lce2 = 0;
            Index lnk2 = 0;

            if(m > 0 && len1 < window_size) {
                auto const isa_cur = isa[pos_to_reverse(m-1)];
                
                auto const marked_l1 = (isa_cur > 0) ? marked.predecessor(MarkedLCP{isa_cur - 1, 0}) : MResult{ false, 0 };
                auto const lce_l1 = marked_l1.exists ? lcp[rmq.queryRMQ(marked_l1.key.sa_pos + 1, isa_cur)] : 0;
                auto const marked_r1 = marked.successor(MarkedLCP{isa_cur + 1, 0});
                auto const lce_r1 = marked_r1.exists ? lcp[rmq.queryRMQ(isa_cur + 1, marked_r1.key.sa_pos)] : 0;

                if(lce_l1 > 0 || lce_r1 > 0) {
                    // find marked position with larger LCE
                    if(lce_l1 > lce_r1) {
                        lnk1 = marked_l1.key.phrase_num;
                        lce1 = lce_l1;
                    } else {
                        lnk1 = marked_r1.key.phrase_num;
                        lce1 = lce_r1;
                    }

                    // additionally, perform queries excluding the end position of the previous phrase
                    if(m > len1 && len2 < window_size) {
                        auto const exclude = z - 1;

                        auto marked_l2 = marked_l1;
                        auto lce_l2 = lce_l1;
                        if(marked_l2.exists && marked_l2.key.phrase_num == exclude) {
                            // ignore end position of previous phrase
                            marked_l2 = (marked_l1.key.sa_pos > 0 ? marked.predecessor(MarkedLCP{marked_l1.key.sa_pos - 1, 0}) : MResult{ false, 0 });
                            lce_l2 = marked_l2.exists ? lcp[rmq.queryRMQ(marked_l2.key.sa_pos + 1, isa_cur)] : 0;
                        }

                        auto marked_r2 = marked_r1;
                        auto lce_r2 = lce_r1;
                        if(marked_r2.exists && marked_r2.key.phrase_num == exclude) {
                            // ignore end position of previous phrase
                            marked_r2 = marked.successor(MarkedLCP{marked_r1.key.sa_pos + 1, 0});
                            lce_r2 = marked_r2.exists ? lcp[rmq.queryRMQ(isa_cur + 1, marked_r2.key.sa_pos)] : 0;
                        }

                        // find marked position with larger LCE
                        if(lce_l2 > 0 || lce_r2 > 0) {
                            if(lce_l2 > lce_r2) {
                                lnk2 = marked_l2.key.phrase_num;
                                lce2 = lce_l2;
                            } else {
                                lnk2 = marked_r2.key.phrase_num;
                                lce2 = lce_r2;
                            }
                        }
                    }

                }
            }

            if(m > len1 && len2 < window_size && lce2 >= len2) {
                // merge the two current phrases and extend their length by one
                if constexpr(DEBUG) std::cout << "\tMERGE phrases " << z << " and " << z-1 << " to new phrase of length " << (lce2+1) << std::endl;

                // updateRecent: unregister current phrase
                unmark(m - 1);

                // updateRecent: unregister previous phrase
                unmark(m - 1 - len1);

                // delete current phrase
                --z;
                assert(z); // nb: must still have at least phrase 0

                // merge phrases
                phrases[z].len = len2 + 1;
                p = lnk2;

                // stats
                ++num_consecutive_merges;
                max_consecutive_merges = std::max(max_consecutive_merges, num_consecutive_merges);
            } else if(m > 0 && len1 < window_size && lce1 >= len1) {
                // extend the current phrase by one character
                if constexpr(DEBUG) std::cout << "\tEXTEND phrase " << z << " to length " << (len1+1) << std::endl;

                // updateRecent: unregister current phrase
                unmark(m - 1);

                ++phrases[z].len;
                p = lnk1;

                // stats
                num_consecutive_merges = 0;
            } else {
                // begin a new phrase of initially length one
                if constexpr(DEBUG) std::cout << "\tNEW phrase " << (z+1) << " of length 1" << std::endl;
                ++z;
                if(z >= phrases.size()) phrases.push_back({0,0,0,0});
                assert(z < phrases.size());

                phrases[z].len = 1;

                // stats
                num_consecutive_merges = 0;
            }

            phrases[z].c = window[m];
            phrases[z].lnk = p;
            // TODO: phrs[z].hash

            if constexpr(DEBUG) std::cout << "\t-> z=" << z << ", lnk=" << phrases[z].lnk << ", len=" << phrases[z].len << ", c=" << display(phrases[z].c) << std::endl;

            // updateRecent: register updated current phrase
            mark(m, z);
            assert(is_marked(m));
        }

        // advance to next step
        ++step;
    }

    // initialize encoding
    out.write(MAGIC, 64);
    PhraseBlockWriter writer(out, block_size, true);

    // write phrases
    {
        size_t i = 0;
        for(size_t j = 1; j <= z; j++) {
            if constexpr(PROTOCOL) std::cout << "factor #" << j << ": i=" << i << ", (" << phrases[j].lnk << ", " << (phrases[j].lnk ? phrases[j].len-1 : 0) << ", " << display(phrases[j].c) << ")" << std::endl;
            i += phrases[j].len;

            ++num_phrases;
            if(phrases[j].len > 1) {
                // referencing phrase
                writer.write_ref(phrases[j].lnk);
                writer.write_len(phrases[j].len - 1);
                writer.write_literal(phrases[j].c);

                ++num_ref;

            } else {
                // literal phrase
                ++num_literal;
                writer.write_ref(0);
                writer.write_literal(phrases[j].c);
            }
            
            longest = std::max(longest, size_t(phrases[j].len));
            total_len += phrases[j].len;
            furthest = std::max(furthest, size_t(phrases[j].lnk));
            total_ref += phrases[j].lnk;
        }
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
