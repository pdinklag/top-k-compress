#include <cassert>
#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/util.hpp>
#include <RMQRMM64.h>

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
    sw.stop();
    if constexpr(TIME_PHASES) { result.add("t_textds", (uint64_t)sw.elapsed_time_millis()); }
    
    // translate a position in the text to the corresponding position in the reverse text (which has a sentinel!)
    auto pos_to_reverse = [&](Index const i) {
        return l - (i+1);
    };

    // compute rmq on LCP array
    if constexpr(TIME_PHASES) sw.start();
    RMQRMM64 rmq((std::make_signed_t<Index>*)lcp.get(), l+1); // argh... the RMQ constructor wants signed integers
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_rmq", (uint64_t)sw.elapsed_time_millis()); }

    // TODO: compute lhash array

    struct PhraseEnding {
        Index num; // the phrase number
        Index end; // the text position at which the phrase ends

        // std::totally_ordered
        bool operator==(PhraseEnding const& x) const { return end == x.end; }
        bool operator!=(PhraseEnding const& x) const { return end != x.end; }
        bool operator< (PhraseEnding const& x) const { return end <  x.end; }
        bool operator<=(PhraseEnding const& x) const { return end <= x.end; }
        bool operator> (PhraseEnding const& x) const { return end >  x.end; }
        bool operator>=(PhraseEnding const& x) const { return end >= x.end; }
    } __attribute__((__packed__));

    // initialize predecessor data structure on phrase end positions
    // -> P contains text positions
    BTree<PhraseEnding, 65> P;

    // initialize "marked binary tree" (a.k.a. predessor + successor data structure)
    // nb: position j is marked iff a phrase ends at position SA[j]
    // -> M contains suffix array positions
    BTree<Index, 65> M;

    // local LZEnd algorithm by Kempa & Kosolobov
    struct Phrase {
        char     c;
        Index    len;
        uint64_t hash;
        Index    lnk;
    };

    std::vector<Phrase> phrs;
    Index z = 0;
    
    auto mark_leaf = [&](Index const x) { M.insert(x); };
    auto unmark_leaf = [&](Index const x) { M.remove(x); };
    auto is_marked = [&](Index const x) { return M.contains(x); };

    auto register_phrase = [&](Index const m, Index const num){
        // register that phrase num ends at text position m
        if constexpr(DEBUG) std::cout << "\tregister phrase " << num << " ending at " << m << std::endl;
        P.insert({num, m});
        mark_leaf(isa[pos_to_reverse(m)]);
    };

    auto unregister_phrase = [&](Index const m){
        // unregister phrase that ends at text position m
        if constexpr(DEBUG) std::cout << "\tunregister phrase ending at " << m << std::endl;
        assert(P.contains({0, m}));
        P.remove({0, Index(m)});

        assert(is_marked(isa[pos_to_reverse(m)]));
        unmark_leaf(isa[pos_to_reverse(m)]);
    };

    struct MarkedLCPResult {
        Index lce; // the LCE between the queried text position and the other position
        Index pos; // the other (text) position
    };

    // find marked LCP closest to text position q using a predecessor and successor query
    // if either is found, compute the LCEs and return the larger one
    auto marked_lcp = [&](Index const q) {
        auto const isa_q = isa[pos_to_reverse(q)];

        // predecessor search for a marked LCP
        auto const r1 = (isa_q > 0) ? M.predecessor(isa_q - 1) : decltype(M)::KeyResult{ false, Index(-1) };

        // successor search for a marked LCP
        auto const r2 = M.successor(isa_q + 1);

        if(!r1.exists && !r2.exists) {
            // there is no suitable marked LCP
            // this case is not considered in the paper, but does occur early in the algorithm
            return MarkedLCPResult{ 0, Index(-1) };
        } else {
            // compute LCE between q and the marked positions (if available)
            Index const lce1 = r1.exists ? lcp[rmq.queryRMQ(r1.key + 1, isa_q)] : 0;
            Index const lce2 = r2.exists ? lcp[rmq.queryRMQ(isa_q + 1, r2.key)] : 0;

            // select the maximum of the two
            return (lce1 > lce2)
                ? MarkedLCPResult { lce1, pos_to_reverse(sa[r1.key]) }
                : MarkedLCPResult { lce2, pos_to_reverse(sa[r2.key]) };
        }
    };

    struct CheckResult {
        bool  exists;
        Index phrase_num;
    };

    // check whether a phrase that ends at position m-1 and whether that phrase is longer than the given length
    auto phrase_ends_at = [&](Index const m, Index const len) {
        assert(m > 0);

        // find the closest marked LCP text position and the LCE with the current position
        auto [lce, pos] = marked_lcp(m - 1);
        if constexpr(DEBUG) std::cout << "\tmarked_lcp(" << (m-1) << ") -> lce=" << lce << ", pos=" << pos << std::endl;
        if(lce < len) {
            // the LCE with the closest marked LCP (if any) is less than the given length, return negative
            return CheckResult { false, 0 };
        } else {
            // the LCE with the closest marked LCP is at least the given length
            // find the phrase pertaining to its position
            auto const r = P.predecessor({0, pos});
            assert(r.exists); // nb: it must exist
            assert(r.key.end == pos); // nb: is this right?

            // return positive with the corresponding phrase number
            return CheckResult { true, r.key.num };
        }
    };

    // test whether a phrase ends at position m-1 and whether that phrase is at least as long as the current phrase
    auto absorbOne2 = [&](Index const m) {
        auto const r = phrase_ends_at(m, phrs[z].len);
        if constexpr(DEBUG) std::cout << "\tabsorbOne2(" << m << ") -> exists=" << r.exists << ", phrase_num=" << r.phrase_num << std::endl;
        return r;
    };

    // test whether a phrase ends at position m-1 and whether that phrase is longer than the two current phrases
    auto absorbTwo2 = [&](Index const m) {
        assert(m > phrs[z].len);
        auto const isa_prev_phrase_end = isa[pos_to_reverse(m - phrs[z].len - 1)];

        // temporarily unmark previous phrase ending
        assert(is_marked(isa_prev_phrase_end)); // nb: make sure the end position of the previous phrase is marked
        unmark_leaf(isa_prev_phrase_end);

        // test whether a phrase - excluding the previous phrase - exists with length at least as long as the two current phrases
        auto const r = phrase_ends_at(m, phrs[z-1].len + phrs[z].len);

        // re-mark previous phrase ending
        mark_leaf(isa_prev_phrase_end);
        if constexpr(DEBUG) std::cout << "\tabsorbTwo2(" << m << ") -> exists=" << r.exists << ", phrase_num=" << r.phrase_num << std::endl;
        return r;
    };

    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;

    if constexpr(TIME_PHASES) sw.start();
    {
        // init phrase to keep code compatible to pseudocode
        phrs.push_back({0,0,0,0});
        z = 0;

        for(Index m = 0; m < l; m++) {
            if constexpr(DEBUG) std::cout << "m=" << m << ", s[m]=" << display(s[m]) << std::endl;
            auto const len = phrs[z].len + (z > 0 ? phrs[z-1].len : 0);
            Index p = 0; // TODO: approxFind in global trie
            
            CheckResult x;
            if(m > phrs[z].len && len < l && (x = absorbTwo2(m)).exists) {
                // merge the two current phrases and extend their length by one
                if constexpr(DEBUG) std::cout << "\tMERGE phrases " << z << " and " << z-1 << " to new phrase of length " << (len+1) << std::endl;

                // updateRecent: unregister current phrase
                unregister_phrase(m - 1);

                // updateRecent: unregister previous phrase
                assert(m > phrs[z].len);
                unregister_phrase(m - 1 - phrs[z].len);

                // delete current phrase
                --z;
                assert(z); // nb: must still have at least one phrase

                // merge phrases
                phrs[z].len = len + 1;
                p = x.phrase_num;
            } else if(m > 0 && phrs[z].len < l && (x = absorbOne2(m)).exists) {
                // extend the current phrase by one
                if constexpr(DEBUG) std::cout << "\tEXTEND phrase " << z << " to length " << (phrs[z].len+1) << std::endl;

                // updateRecent: unregister current phrase
                unregister_phrase(m - 1);

                ++phrs[z].len;
                p = x.phrase_num;
            } else {
                // this introduces a new phrase of length one
                if constexpr(DEBUG) std::cout << "\tNEW phrase " << (z+1) << " of length 1" << std::endl;
                ++z;
                if(z >= phrs.size()) phrs.push_back({0,0,0,0});
                assert(z < phrs.size());

                phrs[z].len = 1;
            }

            phrs[z].c = s[m];
            phrs[z].lnk = p;
            // TODO: phrs[z].hash

            if constexpr(DEBUG) std::cout << "\t-> z=" << z << ", lnk=" << phrs[z].lnk << ", len=" << phrs[z].len << ", c=" << display(phrs[z].c) << std::endl;

            // updateRecent: register new current phrase
            register_phrase(m, z);
        }
    }

    // initialize encoding
    out.write(MAGIC, 64);
    PhraseBlockWriter writer(out, block_size, true);

    // write phrases
    {
        size_t i = 0;
        for(size_t j = 1; j <= z; j++) {
            if constexpr(PROTOCOL) std::cout << "factor #" << j << ": i=" << i << ", (" << phrs[j].lnk << ", " << (phrs[j].lnk ? phrs[j].len-1 : 0) << ", " << display(phrs[j].c) << ")" << std::endl;
            i += phrs[j].len;

            ++num_phrases;
            if(phrs[j].len > 1) {
                // referencing phrase
                writer.write_ref(phrs[j].lnk);
                writer.write_len(phrs[j].len - 1);
                writer.write_literal(phrs[j].c);

                ++num_ref;

            } else {
                // literal phrase
                ++num_literal;
                writer.write_ref(0);
                writer.write_literal(phrs[j].c);
            }
            
            longest = std::max(longest, size_t(phrs[j].len));
            total_len += phrs[j].len;
            furthest = std::max(furthest, size_t(phrs[j].lnk));
            total_ref += phrs[j].lnk;
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
