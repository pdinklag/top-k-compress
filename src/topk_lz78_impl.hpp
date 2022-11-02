#include "topk_common.hpp"

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'Z') << 16 |
    ((uint64_t)'7') << 8 |
    ((uint64_t)'8');

constexpr bool PROTOCOL = false;

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_lz78(In begin, In const& end, Out out, size_t const k, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    TopkFormat f(k, 0 /* indicator for LZ78 compression :-) */, num_sketches, sketch_rows, sketch_columns, false);
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

    Topk::StringState s = topk.empty_string();
    auto handle = [&](char const c) {
        auto next = topk.extend(s, c);
        if(!next.frequent) {
            longest = std::max(longest, size_t(next.len));
            writer.write_ref(s.node);
            writer.write_literal(c);

            if constexpr(PROTOCOL) std::cout << "(" << s.node << ") 0x" << std::hex << (size_t)c << std::dec << std::endl;

            s = topk.empty_string();
            ++num_phrases;
        } else {
            s = next;
        }
    };

    while(begin != end) {
        // read next character
        ++n;
        handle(*begin++);
    }

    // encode final phrase, if any
    if(s.len > 0) {
        writer.write_ref(s.node);
        ++num_phrases;

        if constexpr(PROTOCOL) std::cout << "(" << s.node << ")" << std::endl;
    }

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

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_lz78(In in, Out out) {
    using namespace tdc::code;

    // decode header
    TopkFormat f(in, MAGIC);
    auto const k = f.k;
    auto const window_size = f.window_size;
    auto const num_sketches = f.num_sketches;
    auto const sketch_rows = f.sketch_rows;
    auto const sketch_columns = f.sketch_columns;
    auto const huffman_coding = f.huffman_coding;

    // initialize decompression
    // - frequent substring 0 is reserved to indicate a literal character
    using Topk = TopKSubstrings<TopkTrieNode<>>;
    Topk topk(k, num_sketches, sketch_rows, sketch_columns);

    size_t n = 0;
    size_t num_phrases = 0;

    // initialize decoding
    PhraseBlockReader reader(in);

    char* phrase = new char[k]; // phrases can be of length up to k...
    while(in) {
        // decode and handle phrase
        auto const x = reader.read_ref();
        if constexpr(PROTOCOL) std::cout << "(" << x << ")";

        auto const phrase_len = topk.get(x, phrase);
        
        ++num_phrases;
        n += phrase_len;

        auto s = topk.empty_string();
        for(size_t i = 0; i < phrase_len; i++) {
            auto const c = phrase[i];
            s = topk.extend(s, c);
            *out++ = c;
        }

        // decode and handle literal
        if(in)
        {
            auto const literal = reader.read_literal();
            topk.extend(s, literal);
            *out++ = literal;
            ++n;

            if constexpr(PROTOCOL) std::cout << " 0x" << std::hex << (size_t)literal << std::dec;
        }

        if constexpr(PROTOCOL) std::cout << std::endl;
    }

    std::cout << "num_phrases=" << num_phrases << " -> n=" << n << std::endl;
}
