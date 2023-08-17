#include <vector>

#include <iopp/concepts.hpp>

#include <block_coding.hpp>
#include <trie_fcns.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'7') << 40 |
    ((uint64_t)'8') << 32 |
    ((uint64_t)'F') << 24 |
    ((uint64_t)'U') << 16 |
    ((uint64_t)'L') << 8 |
    ((uint64_t)'L');

constexpr TokenType TOK_TRIE_REF = 0;
constexpr TokenType TOK_LITERAL = 1;

void setup_encoding(BlockEncodingBase& enc) {
    enc.register_binary(SIZE_MAX); // TOK_TRIE_REF
    enc.register_huffman();        // TOK_LITERAL
}

template<iopp::InputIterator<char> In, iopp::BitSink Out>
void lz78_compress(In begin, In const& end, Out out, size_t const block_size, pm::Result& result) {
    out.write(MAGIC, 64);

    BlockEncoder enc(out, block_size);
    setup_encoding(enc);

    TrieFCNS trie;
    TrieFCNS::Node u = trie.root();
    size_t d = 0;
    TrieFCNS::Node v;

    size_t num_phrases = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;

    while(begin != end) {
        auto const c = *begin++;

        if(trie.follow_edge(u, c, v)) {
            // edge exists, extend phrase
            u = v;
            ++d;
        } else {
            // edge doesn't exist, new LZ78 phrase
            enc.write_uint(TOK_TRIE_REF, u);
            enc.write_char(TOK_LITERAL, c);

            ++num_phrases;
            longest = std::max(longest, d);
            total_len += d;
            furthest = std::max(furthest, u);
            total_ref += u;

            u = trie.root();
            d = 0;
        }
    }

    // encode final phrase, if any
    if(u) {
        enc.write_uint(TOK_TRIE_REF, u);
        ++num_phrases; // final phrase
    }

    enc.flush();

    // stats
    result.add("phrases_total", num_phrases);
    result.add("phrases_longest", longest);
    result.add("phrases_furthest", furthest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("phrases_avg_dist", std::round(100.0 * ((double)total_ref / (double)num_phrases)) / 100.0);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void lz78_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    std::string s; // yes, we do it in RAM...
    std::vector<std::pair<size_t, char>> factors;
    factors.emplace_back(0, 0); // align

    std::function<void(size_t const)> decode;
    decode = [&](size_t const f){
        if(f) {
            decode(factors[f].first);
            s.push_back(factors[f].second);
        }
    };

    BlockDecoder dec(in);
    setup_encoding(dec);
    while(in) {
        auto const f = dec.read_uint(TOK_TRIE_REF);
        decode(f);

        if(in) {
            auto const c = dec.read_char(TOK_LITERAL);
            s.push_back(c);
            factors.emplace_back(f, c);
        }
    }

    // output
    std::copy(s.begin(), s.end(), out);
}
