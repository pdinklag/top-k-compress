#include <cassert>
#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/suffix_array.hpp>
#include <RMQRMM64.h>

#include <pm/stopwatch.hpp>

#include <display.hpp>

#include <index/backward_search.hpp>
#include <index/btree.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

constexpr bool PROTOCOL = false;
constexpr bool TIME_PHASES = false;
constexpr bool TIME_OPS = false;

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
    pm::Stopwatch sw;

    // fully read file into RAM
    std::string s;
    std::copy(begin, end, std::back_inserter(s));
    
    Index const n = s.length();
    
    // compute suffix array of reverse text
    if constexpr(TIME_PHASES) sw.start();
    std::string r;
    r.reserve(n+1);
    std::copy(s.rbegin(), s.rend(), std::back_inserter(r));
    r.push_back(0); // make sure that the last suffix is the lexicographically smallest
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_revert", (uint64_t)sw.elapsed_time_millis()); }
    
    if constexpr(TIME_PHASES) sw.start();
    auto sa = std::make_unique<Index[]>(n+1);
    tdc::text::suffix_array(r.begin(), r.end(), sa.get());
    assert(sa[0] == n);
    sw.stop();
    if constexpr(TIME_PHASES) { result.add("t_sa", (uint64_t)sw.elapsed_time_millis()); }
    
    // build rMq index on suffix array
    // Ferrada's RMQ library only allows range MINIMUM queries, but we need range MAXIMUM
    // to resolve this, we create a temporary copy of the suffix array with all values negated
    if constexpr(TIME_PHASES) sw.start();
    size_t const log_n = std::bit_width(n); // n+1 - 1
    auto sa_neg = std::make_unique<SIndex[]>(n+1);
    for(size_t i = 0; i < n+1; i++) sa_neg[i] = -SIndex(sa[i]);
    RMQRMM64 rMq(sa_neg.get(), n+1);
    sa_neg.reset(); // we cannot scope this because RMQRMM64 does not feature default / move construction
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_rmq", (uint64_t)sw.elapsed_time_millis()); }
    
    // compute inverse suffix array
    if constexpr(TIME_PHASES) sw.start();
    auto isa = std::make_unique<Index[]>(n+1);
    for(size_t i = 0; i < n+1; i++) isa[sa[i]] = i;
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_isa", (uint64_t)sw.elapsed_time_millis()); }
    
    // compute BWT of reverse text
    if constexpr(TIME_PHASES) sw.start();
    std::string bwt;
    bwt.reserve(n+1);
    for(size_t i = 0; i < n+1; i++) {
        auto const j = sa[i];
        bwt.push_back(j ? r[j-1] : r[n]);
    }
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_bwt", (uint64_t)sw.elapsed_time_millis()); }
    
    // build backward search index on BWT
    if constexpr(TIME_PHASES) sw.start();
    BackwardSearch bws(bwt.begin(), bwt.end());
    if constexpr(TIME_PHASES) { sw.stop(); result.add("t_bws", (uint64_t)sw.elapsed_time_millis()); }
    
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
    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;
    
    uint64_t t_bws_step = 0;
    uint64_t t_rmq = 0;
    uint64_t t_succ_query = 0;
    uint64_t t_succ_insert = 0;
    pm::Stopwatch sw_detail;

    if constexpr(TIME_PHASES) sw.start();
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
                if constexpr(TIME_OPS) { sw_detail.start(); }
                x = bws.step(x, s[i_]);
                if constexpr(TIME_OPS) { sw_detail.stop(); t_bws_step += sw_detail.elapsed_time_nanos(); }

                if constexpr(TIME_OPS) { sw_detail.start(); }
                auto const mpos = (x.first <= x.second) ? rMq.queryRMQ(x.first, x.second) : 0;
                if constexpr(TIME_OPS) { sw_detail.stop(); t_rmq += sw_detail.elapsed_time_nanos(); }
                
                if(sa[mpos] <= pos_to_reverse(i)) break;
                ++i_;
                
                if constexpr(TIME_OPS) { sw_detail.start(); }
                auto const f = factors.successor({0, (Index)x.first}).key;
                if constexpr(TIME_OPS) { sw_detail.stop(); t_succ_query += sw_detail.elapsed_time_nanos(); }
                if(f.pos <= x.second) {
                    assert(f.num > 0);

                    j = i_;
                    q = f.num;
                }
            }
            
            if constexpr(TIME_OPS) { sw_detail.start(); }
            if(j < n) factors.insert({p, isa[pos_to_reverse(j)]});
            if constexpr(TIME_OPS) { sw_detail.stop(); t_succ_insert += sw_detail.elapsed_time_nanos(); }

            ++num_phrases;
            writer.write_ref(q);
            if(q > 0) writer.write_len(j-i);
            if(j < n) writer.write_literal(s[j]);
            
            if(q > 0) ++num_ref;
            else ++num_literal;
            
            longest = std::max(longest, size_t(j-i+1));
            total_len += j-i+1;
            
            furthest = std::max(furthest, size_t(q));
            total_ref += q;
            
            if constexpr(PROTOCOL) {
                std::cout << "factor #" << p << ": i=" << i << ", (" << q << ", " << (q > 0 ? j-i : 0) << ", " << (j < n ? display(s[j]) : "<EOF>") << ")" << std::endl;
            }
            
            i = j + 1;
            ++p;
        }
    }

    // flush
    writer.flush();

    if constexpr(TIME_PHASES) {
        sw.stop();
        result.add("t_compress", (uint64_t)sw.elapsed_time_millis());
    }

    if constexpr(TIME_OPS) {
        result.add("t_compress_bws_step", t_bws_step / 1'000'000);
        result.add("t_compress_rmq", t_rmq / 1'000'000);
        result.add("t_compress_succ_insert", t_succ_insert / 1'000'000);
        result.add("t_compress_succ_query", t_succ_query / 1'000'000);
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
