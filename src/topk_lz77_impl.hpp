#include "topk_common.hpp"

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'Z') << 16 |
    ((uint64_t)'7') << 8 |
    ((uint64_t)'7');

constexpr bool PROTOCOL = true;

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_lz78(In begin, In const& end, Out out, size_t const k, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    TopkFormat f(k, 0, num_sketches, sketch_rows, sketch_columns, false);
    f.encode_header(out, MAGIC);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    using Topk = TopKSubstrings<TopkTrieNode<>>;
    Topk topk(k, num_sketches, sketch_rows, sketch_columns);
    size_t n = 0;
    size_t num_phrases = 0;
    size_t longest = 0;

    // initialize encoding
    PhraseBlockWriter writer(out, block_size);

    auto handle = [&](char const c) {
        // TODO
        /*
            VARIABLES
                S: buffer of read text
                v: *current* node
                x: *factor* node

            for each character c:
                from v, traverse up the trie and find node with Weiner link labeled c
                v := reached node
                if v has a Weiner link for c:
                    follow link
                    descend down its subtrie using characters of S in reverse order, starting from position i-1-d(v)
                    v := reached node
                else:
                    (v must be the root and has no edge with label c)
                
                if x is NOT the root and d(v) <= d(x):
                    extend current LZ factor
                else:
                    emit current LZ factor (get source position from x)
                    begin new LZ factor

                x := v

                if x is the root:
                    emit literal factor c
        */
    };

    while(begin != end) {
        // read next character
        ++n;
        handle(*begin++);
    }

    // TODO: potentially encode final phrase

    writer.flush();
    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << n
        << " -> num_phrases=" << num_phrases
        << ", longest=" << longest
        << std::endl;
}
