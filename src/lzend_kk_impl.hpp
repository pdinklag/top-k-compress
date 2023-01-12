#include <cassert>
#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/util.hpp>
#include <RMQRMM64.h>
#include <ankerl/unordered_dense.h>

#include <pm/stopwatch.hpp>

#include <display.hpp>

#include <index/backward_search.hpp>
#include <index/btree.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

constexpr bool DEBUG = false;
constexpr bool PROTOCOL = false;
constexpr bool TIME_PHASES = false;

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
using SIndex = std::make_signed_t<Index>;

struct Factor {
    Index num;
    Index pos;
    
    // std::totally_ordered
    bool operator==(Factor const& x) const { return pos == x.pos; }
    bool operator!=(Factor const& x) const { return pos != x.pos; }
    bool operator< (Factor const& x) const { return pos <  x.pos; }
    bool operator<=(Factor const& x) const { return pos <= x.pos; }
    bool operator> (Factor const& x) const { return pos >  x.pos; }
    bool operator>=(Factor const& x) const { return pos >= x.pos; }
} __attribute__((__packed__));

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void lzend_kk_compress(In begin, In const& end, Out out, size_t const block_size, pm::Result& result) {
    pm::Stopwatch sw;

    // fully read file into RAM (TODO: only window by window)
    std::string s;
    std::copy(begin, end, std::back_inserter(s));
    
    Index const l = s.length();
    
    // compute suffix array, inverse and LCP array of reverse text
    if constexpr(TIME_PHASES) sw.start();
    std::string r;
    r.reserve(l+1);
    std::copy(s.rbegin(), s.rend(), std::back_inserter(r));
    r.push_back(0); // make sure that the last suffix is the lexicographically smallest
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_revert", (uint64_t)sw.elapsed_time_millis()); }

    if constexpr(TIME_PHASES) sw.start();
    auto [sa, isa, lcp] = tdc::text::sa_isa_lcp_u32(r.begin(), r.end());
    assert(sa[0] == l);
    sa.reset(); // the suffix array itself is never actually needed and can be discarded
    sw.stop();
    if constexpr(TIME_PHASES) { result.add("t_textds", (uint64_t)sw.elapsed_time_millis()); }
    
    // translate a position in the text to the corresponding position in the reverse text (which has a sentinel!)
    auto pos_to_reverse = [&](Index const i) { return l - (i+1); };

    // compute rmq on LCP array
    if constexpr(TIME_PHASES) sw.start();
    RMQRMM64 rmq((std::make_signed_t<Index>*)lcp.get(), l+1); // argh... the RMQ constructor wants signed integers
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_rmq", (uint64_t)sw.elapsed_time_millis()); }

    // TODO: compute lhash array

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

    // initialize "marked binary tree" (a.k.a. predessor + successor data structure)
    // nb: position j is marked iff a phrase ends at position SA[j]
    // -> M contains suffix array positions
    // nb: the "balanced tree" P is also simulated by this data structure, which we modified to mark positions along with the corresponding phrase number
    BTree<MarkedLCP, 65> M;

    using MResult = decltype(M)::KeyResult;

    // local LZEnd algorithm by Kempa & Kosolobov
    struct Phrase {
        char     c;
        Index    len;
        uint64_t hash;
        Index    lnk;
    };

    std::vector<Phrase> phrases;
    Index z = 0;
    
    auto mark = [&](Index const pos, Index const phrase_num){
        // register that phrase phrase_num ends at text position pos
        if constexpr(DEBUG) std::cout << "\tregister phrase " << phrase_num << " ending at " << pos << std::endl;
        auto const isa_m = isa[pos_to_reverse(pos)];
        M.insert(MarkedLCP{isa_m, phrase_num});
    };

    auto unmark = [&](Index const m){
        // unregister phrase that ends at text position m
        if constexpr(DEBUG) std::cout << "\tunregister phrase ending at " << m << std::endl;
        auto const isa_m = isa[pos_to_reverse(m)];
        assert(M.contains(MarkedLCP{isa_m, 0}));
        M.remove(MarkedLCP{isa_m, 0});
    };

    #ifndef NDEBUG
    auto is_marked = [&](Index const m) {
        auto const isa_m = isa[pos_to_reverse(m)];
        return M.contains(MarkedLCP{isa_m, 0});
    };
    #endif

    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;
    size_t max_consecutive_merges = 0;
    size_t num_consecutive_merges = 0;

    if constexpr(TIME_PHASES) sw.start();
    {
        // init phrase to keep code compatible to pseudocode
        phrases.push_back({0,0,0,0});
        z = 0;

        for(Index m = 0; m < l; m++) {
            if constexpr(DEBUG) std::cout << "m=" << m << ", s[m]=" << display(s[m]) << std::endl;

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

            if(m > 0) {
                auto const isa_cur = isa[pos_to_reverse(m-1)];
                
                auto const marked_l1 = (isa_cur > 0) ? M.predecessor(MarkedLCP{isa_cur - 1, 0}) : MResult{ false, 0 };
                auto const lce_l1 = marked_l1.exists ? lcp[rmq.queryRMQ(marked_l1.key.sa_pos + 1, isa_cur)] : 0;
                auto const marked_r1 = M.successor(MarkedLCP{isa_cur + 1, 0});
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
                    if(m > len1) {
                        auto const exclude = z - 1;

                        auto marked_l2 = marked_l1;
                        auto lce_l2 = lce_l1;
                        if(marked_l2.exists && marked_l2.key.phrase_num == exclude) {
                            // ignore end position of previous phrase
                            marked_l2 = (marked_l1.key.sa_pos > 0 ? M.predecessor(MarkedLCP{marked_l1.key.sa_pos - 1, 0}) : MResult{ false, 0 });
                            lce_l2 = marked_l2.exists ? lcp[rmq.queryRMQ(marked_l2.key.sa_pos + 1, isa_cur)] : 0;
                        }

                        auto marked_r2 = marked_r1;
                        auto lce_r2 = lce_r1;
                        if(marked_r2.exists && marked_r2.key.phrase_num == exclude) {
                            // ignore end position of previous phrase
                            marked_r2 = M.successor(MarkedLCP{marked_r1.key.sa_pos + 1, 0});
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

            if(m > len1 && len1 < l && lce2 >= len2) {
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
            } else if(m > 0 && len1 < l && lce1 >= len1) {
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

            phrases[z].c = s[m];
            phrases[z].lnk = p;
            // TODO: phrs[z].hash

            if constexpr(DEBUG) std::cout << "\t-> z=" << z << ", lnk=" << phrases[z].lnk << ", len=" << phrases[z].len << ", c=" << display(phrases[z].c) << std::endl;

            // updateRecent: register updated current phrase
            mark(m, z);
            assert(is_marked(m));
        }
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

    if constexpr(TIME_PHASES) {
        sw.stop();
        result.add("t_compress", (uint64_t)sw.elapsed_time_millis());
    }
    
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
