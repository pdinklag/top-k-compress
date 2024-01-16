#include <block_coding.hpp>
#include <pm/result.hpp>

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

constexpr TokenType TOK_TRIE_REF = 0;
constexpr TokenType TOK_LITERAL = 1;

void setup_encoding(BlockEncodingBase& enc, size_t const k) {
    enc.register_binary(k-1); // TOK_TRIE_REF
    enc.register_huffman();   // TOK_LITERAL
}

template<typename Topk, iopp::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_lz78(In begin, In const& end, Out out, size_t const k, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    out.write(MAGIC, 64);
    out.write(k, 64);
    out.write(sketch_columns, 64);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    
    Topk topk(k - 1, sketch_columns);
    size_t n = 0;
    size_t num_phrases = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;

    // initialize encoding
    BlockEncoder enc(out, block_size);
    setup_encoding(enc, k);

    auto s = topk.empty_string();
    auto handle = [&](char const c) {
        auto next = topk.extend(s, c);
        if(!next.frequent) {
            longest = std::max(longest, size_t(next.len));
            total_len += next.len;
            furthest = std::max(furthest, size_t(s.node));
            total_ref += s.node;
            enc.write_uint(TOK_TRIE_REF, s.node);
            enc.write_char(TOK_LITERAL, c);

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
        enc.write_uint(TOK_TRIE_REF, s.node);
        ++num_phrases;

        if constexpr(PROTOCOL) std::cout << "(" << s.node << ")" << std::endl;
    }

    enc.flush();
    
    // stats
    topk.print_debug_info();
    result.add("phrases_total", num_phrases);
    result.add("phrases_longest", longest);
    result.add("phrases_furthest", furthest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("phrases_avg_dist", std::round(100.0 * ((double)total_ref / (double)num_phrases)) / 100.0);
}

template<typename Topk, iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_lz78(In in, Out out) {
    // decode header
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    auto const k = in.read(64);
    auto const sketch_columns = in.read(64);

    // initialize decompression
    // - frequent substring 0 is reserved to indicate a literal character
    Topk topk(k - 1, sketch_columns);

    size_t n = 0;
    size_t num_phrases = 0;

    // initialize decoding
    BlockDecoder dec(in);
    setup_encoding(dec, k);

    char* phrase = new char[k]; // phrases can be of length up to k...
    while(in) {
        // decode and handle phrase
        auto const x = dec.read_uint(TOK_TRIE_REF);
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
            auto const literal = dec.read_char(TOK_LITERAL);
            topk.extend(s, literal);
            *out++ = literal;
            ++n;

            if constexpr(PROTOCOL) std::cout << " 0x" << std::hex << (size_t)literal << std::dec;
        }

        if constexpr(PROTOCOL) std::cout << std::endl;
    }
}
