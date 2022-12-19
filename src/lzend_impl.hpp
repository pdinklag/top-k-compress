#include <cassert>
#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/suffix_array.hpp>
#include <RMQRMM64.h>

#include <display.hpp>

#include <index/backward_search.hpp>
#include <index/btree.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'E') << 40 |
    ((uint64_t)'N') << 32 |
    ((uint64_t)'D') << 24 |
    ((uint64_t)'#') << 16 |
    ((uint64_t)'#') << 8 |
    ((uint64_t)'#');

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
void lzend_compress(In begin, In const& end, Out out, size_t const block_size, pm::Result& result) {
    // fully read file into RAM
    std::string s;
    std::copy(begin, end, std::back_inserter(s));
    
    Index const n = s.length();
    
    // compute suffix array of reverse text
    std::string r;
    r.reserve(n+1);
    std::copy(s.rbegin(), s.rend(), std::back_inserter(r));
    r.push_back(0); // make sure that the last suffix is the lexicographically smallest
    
    auto sa = std::make_unique<Index[]>(n+1);
    tdc::text::suffix_array(r.begin(), r.end(), sa.get());
    assert(sa[0] == n);
    
    // build rMq index on suffix array
    // Ferrada's RMQ library only allows range MINIMUM queries, but we need range MAXIMUM
    // to resolve this, we create a temporary copy of the suffix array with all values negated
    size_t const log_n = std::bit_width(n); // n+1 - 1
    auto sa_neg = std::make_unique<SIndex[]>(n+1);
    for(size_t i = 0; i < n+1; i++) sa_neg[i] = -SIndex(sa[i]);
    RMQRMM64 rMq(sa_neg.get(), n+1);
    sa_neg.reset(); // we cannot scope this because RMQRMM64 does not feature default / move construction
    
    // compute inverse suffix array
    auto isa = std::make_unique<Index[]>(n+1);
    for(size_t i = 0; i < n+1; i++) isa[sa[i]] = i;
    
    // compute BWT of reverse text
    std::string bwt;
    bwt.reserve(n+1);
    for(size_t i = 0; i < n+1; i++) {
        auto const j = sa[i];
        bwt.push_back(j ? r[j-1] : r[n]);
    }
    
    // build backward search index on BWT
    BackwardSearch bws(bwt.begin(), bwt.end());
    
    // initialize dynamic successor data structure
    BTree<Factor, 65> factors;
    
    // initialize encoding
    out.write(MAGIC, 64);
    PhraseBlockWriter writer(out, block_size, true);
    
    // translate a position in the text to the corresponding position in the reverse text (which has a sentinel!)
    auto pos_to_reverse = [&](size_t const i) {
        return n - (i+1);
    };

    // LZEnd algorithm
    {
        using Interval = BackwardSearch<>::Interval;

        factors.insert({0, n+1}); // ensure there is always a successor
        Index i = 0;
        Index p = 1;
        while(i < n) {
            Interval x = {0, n};
            Index i_ = i;
            Index j = i;
            Index q = 0;
            while(i_ < n) {
                x = bws.step(x, s[i_]);
                auto const mpos = (x.first <= x.second) ? rMq.queryRMQ(x.first, x.second) : 0;
                
                if(sa[mpos] <= pos_to_reverse(i)) break;
                ++i_;
                
                auto const f = factors.successor({0, (Index)x.first}).key;
                if(f.pos <= x.second) {
                    j = i_;
                    q = f.num;
                }
            }
            
            if(j < n) factors.insert({p, isa[pos_to_reverse(j)]});

            writer.write_ref(q);
            if(q > 0) writer.write_len(j-i);
            if(j < n) writer.write_literal(s[j]);
            
            i = j + 1;
            ++p;
        }
    }

    // flush
    writer.flush();
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void lzend_decompress(In in, Out out) {
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
        if(q > 0) {
            auto const len = reader.read_len();
            auto p = factors[q-1] + 1 - len;
            for(size_t i = 0; i < len; i++) {
                dec.push_back(dec[p++]);
            }
        }
        
        if(in) {
            auto const c = reader.read_literal();
            factors.push_back(dec.length());
            dec.push_back(c);
        }
    }

    // output
    std::copy(dec.begin(), dec.end(), out);
}
