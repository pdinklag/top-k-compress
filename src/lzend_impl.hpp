#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/suffix_array.hpp>
#include <RMQRMM64.h>

#include "index/backward_search.hpp"

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
static_assert(std::numeric_limits<Index>::digits <= std::numeric_limits<ulong>::digits); // for rMq

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void lzend_compress(In begin, In const& end, Out out, size_t const block_size, pm::Result& result) {
    // fully read file into RAM
    std::string s;
    std::copy(begin, end, std::back_inserter(s));
    size_t const n = s.length();
    size_t const log_n = std::bit_width(n-1);
    
    // compute suffix array
    auto sa = std::make_unique<Index[]>(n);
    tdc::text::suffix_array(s.begin(), s.end(), sa.get());
    
    // build rMq index on suffix array
    // Ferrada's RMQ library only allows range MINIMUM queries, but we need range MAXIMUM
    // to resolve this, we create a temporary copy of the suffix array with all values bit flipped
    auto sa_flipped = std::make_unique<ulong[]>(n);
    for(size_t i = 0; i < n; i++) sa_flipped[i] = ~sa[i];
    RMQRMM64 rMq(sa_flipped.get(), log_n, n, false);
    sa_flipped.reset(); // we cannot scope this because RMQRMM64 does not feature default / move construction
    
    // compute inverse suffix array
    auto isa = std::make_unique<Index[]>(n);
    for(size_t i = 0; i < n; i++) isa[sa[i]] = i;
    
    // compute BWT
    std::string bwt;
    bwt.reserve(n);
    for(size_t i = 0; i < n; i++) {
        auto const j = sa[i];
        bwt[i] = j ? s[j-1] : s[n-1];
    }
    
    // build backward search index on BWT
    BackwardSearch bws(bwt.begin(), bwt.end());
    
    // TODO: initialize dynamic successor data structure

    // TODO: implement

    out.write(MAGIC, 64);
    PhraseBlockWriter writer(out, block_size);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void lzend_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }
}
