#pragma once

#include <concepts>
#include <cstdint>
#include <cstddef>
#include <memory>

#include <alx_rmq.hpp>
#include <fp_string_view.hpp>
#include <index/btree.hpp>
#include <index/dynamic_universe_sampling.hpp>
#include <tdc/text/util.hpp>

template<std::unsigned_integral Index>
class LZEndWindowIndex {
private:
    using FPString = FPStringView<char>;
    using RMQ = alx::rmq::rmq_n<Index, Index>;
    using MarkResult = KeyValueResult<Index, Index>;

    size_t window_size;
    std::string rwindow;

    std::unique_ptr<uint32_t[]> lcp;
    std::unique_ptr<uint32_t[]> isa;
    RMQ rmq;
    
    //BTree<Index, Index, 65> marked;
    DynamicUniverseSampling<Index, Index, 4096> marked;

    FPString rfp;

public:
    LZEndWindowIndex(std::string_view window) : window_size(window.size()), marked(window_size+1) {
        // reverse window
        rwindow.reserve(window_size+1);
        std::copy(window.rbegin(), window.rend(), std::back_inserter(rwindow));
        rwindow.push_back(0); // make sure that the last suffix is the lexicographically smallest
        assert(rwindow.size() == window_size + 1);

        // compute inverse suffix array and LCP array of reverse window
        {
            auto [_sa, _isa, _lcp] = tdc::text::sa_isa_lcp_u32(rwindow.begin(), rwindow.end());
            assert(_sa[0] == window_size);

            // keep inverse suffix array and LCP array, discard suffix array and reversed window
            lcp = std::move(_lcp);
            isa = std::move(_isa);
        }

        // initialize RMQ
        rmq = RMQ(lcp.get(), rwindow.size());

        // initialize fingerprinting
        rfp = FPString(rwindow);
    }

    // translate a position in the block to the corresponding position in the reverse block (which has a sentinel!)
    Index pos_to_reverse(Index const i) const {
        return window_size - (i+1);
    }

    bool is_marked(Index const m) const {
        auto const isa_m = isa[pos_to_reverse(m)];
        return marked.contains(isa_m);
    }

    void mark(Index const m, Index const phrase_num, bool silent = false){
        assert(!is_marked(m));
        auto const isa_m = isa[pos_to_reverse(m)];
        marked.insert(isa_m, phrase_num);
    }

    void unmark(Index const m, bool silent = false) {
        auto const isa_m = isa[pos_to_reverse(m)];
        assert(marked.contains(isa_m));
        marked.remove(isa_m);
    }

    void clear_marked() {
        marked.clear();
    }

    void marked_lcp(Index const q, Index& lnk, Index& lce) const {
        auto const isa_q = isa[pos_to_reverse(q)];

        // look for the marked LCPs sorrounding the suffix array position of q
        auto const marked_l = (isa_q > 0) ? marked.predecessor(isa_q - 1) : MarkResult::none();
        auto const lce_l = marked_l.exists ? lcp[rmq.rmq(marked_l.key + 1, isa_q)] : 0;
        auto const marked_r = marked.successor(isa_q + 1);
        auto const lce_r = marked_r.exists ? lcp[rmq.rmq(isa_q + 1, marked_r.key)] : 0;

        // select the longer LCP and return it along with the corresponding phrase number
        if(lce_l > lce_r) {
            lnk = marked_l.value;
            lce = lce_l;
        } else {
            lnk = marked_r.value;
            lce = lce_r;
        }
    }

    void marked_lcp2(Index const q, Index const exclude, Index& lnk1, Index& lce1, Index& lnk2, Index& lce2) const {
        // init
        lnk1 = 0;
        lce1 = 0;
        lnk2 = 0;
        lce2 = 0;

        // this is basically marked_lcp, except we want to keep all intermediate results for further computation of absorbTwo2
        auto const isa_q = isa[pos_to_reverse(q)];
        auto const marked_l1 = (isa_q > 0) ? marked.predecessor(isa_q - 1) : MarkResult::none();
        auto const lce_l1 = marked_l1.exists ? lcp[rmq.rmq(marked_l1.key + 1, isa_q)] : 0;
        auto const marked_r1 = marked.successor(isa_q + 1);
        auto const lce_r1 = marked_r1.exists ? lcp[rmq.rmq(isa_q + 1, marked_r1.key)] : 0;

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
            auto marked_l2 = marked_l1;
            auto lce_l2 = lce_l1;
            if(marked_l2.exists && marked_l2.value == exclude) {
                // ignore end position of previous phrase
                marked_l2 = (marked_l1.key > 0 ? marked.predecessor(marked_l1.key - 1) : MarkResult::none());
                lce_l2 = marked_l2.exists ? lcp[rmq.rmq(marked_l2.key + 1, isa_q)] : 0;
            }

            auto marked_r2 = marked_r1;
            auto lce_r2 = lce_r1;
            if(marked_r2.exists && marked_r2.value == exclude) {
                // ignore end position of previous phrase
                marked_r2 = marked.successor(marked_r1.key + 1);
                lce_r2 = marked_r2.exists ? lcp[rmq.rmq(isa_q + 1, marked_r2.key)] : 0;
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

    FPString const& reverse_fingerprints() const { return rfp; }

    uint64_t reverse_fingerprint(Index const beg, Index const end) const {
        assert(beg <= end);
        return rfp.fingerprint(pos_to_reverse(end), pos_to_reverse(beg));
    }

    size_t size() const { return rwindow.size(); }

    struct MemoryProfile {
        inline static MemoryProfile max(MemoryProfile const& a, MemoryProfile const& b) {
            MemoryProfile max;
            max.reverse_window = std::max(a.reverse_window, b.reverse_window);
            max.lcp_isa = std::max(a.lcp_isa, b.lcp_isa);
            max.tmp_sa = std::max(a.tmp_sa, b.tmp_sa);
            max.marked = std::max(a.marked, b.marked);
            max.rmq = std::max(a.rmq, b.rmq);
            max.fingerprints = std::max(a.fingerprints, b.fingerprints);
            return max;
        }

        size_t reverse_window = 0;
        size_t lcp_isa = 0;
        size_t tmp_sa = 0;
        size_t marked = 0;
        size_t rmq = 0;
        size_t fingerprints = 0;

        inline size_t total() const { return reverse_window + lcp_isa + tmp_sa + marked + rmq + fingerprints; }
    };

    MemoryProfile memory_profile() const {
        MemoryProfile profile;
        profile.reverse_window = rwindow.capacity() * sizeof(char);
        profile.lcp_isa = (rwindow.size() + 1) * (sizeof(uint32_t) + sizeof(uint32_t)); // lcp and isa
        profile.tmp_sa = (rwindow.size() + 1) * sizeof(uint32_t);
        profile.rmq = rmq.memory_size();
        profile.marked = marked.memory_size();
        profile.fingerprints = rfp.memory_size();
        return profile;
    }
};

