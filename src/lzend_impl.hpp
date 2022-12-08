#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <tdc/text/suffix_array.hpp>
#include <RMQRMM64.h>

#include "index/wavelet_tree.hpp"

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

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void lzend_compress(In begin, In const& end, Out out, size_t const block_size, pm::Result& result) {
    // TODO: compute suffix array, rMq on suffix array, BWT, backward search index on BWT, inverse suffix array
    
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
