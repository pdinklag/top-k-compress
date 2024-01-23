#include <lz77/lpf_factorizer.hpp>

#include <block_coding.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'7') << 40 |
    ((uint64_t)'7') << 32 |
    ((uint64_t)'B') << 24 |
    ((uint64_t)'L') << 16 |
    ((uint64_t)'C') << 8 |
    ((uint64_t)'K');

using Index = uint32_t;

constexpr bool PROTOCOL = false;

// nb: we use different token types to encode references
// the first token of any phrase is always the length:
// - a length of one indicates a literal character
// - otherwise, we have a block-local LZ77 factor of the corresponding length
constexpr TokenType TOK_FACT_SRC = 0;
constexpr TokenType TOK_FACT_LEN = 1;
constexpr TokenType TOK_LITERAL = 2;
constexpr TokenType TOK_FACT_REMAINDER = 3;

constexpr size_t MAX_LZ_REF_LEN = 255;

void setup_encoding(BlockEncodingBase& enc, size_t const window_size) {
    enc.register_binary(window_size-1); // TOK_FACT_SRC
    enc.register_rans();                // TOK_FACT_LEN
    enc.register_binary(255, false);    // TOK_FACT_LITERAL
    enc.register_binary(window_size, false); // TOK_FACT_REMAINDER
}

template<iopp::InputIterator<char> In, iopp::BitSink Out>
void compress_lz77_blockwise(In begin, In const& end, Out out, size_t const threshold, size_t const window_size, size_t const block_size, pm::Result& result) {
    // init stats
    size_t num_ref = 0;
    size_t num_trie = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_ref_len = 0;
    size_t num_relevant = 0;

    // write header and initialize encoding
    out.write(MAGIC, 64);
    out.write(window_size, 64);

    BlockEncoder enc(out, block_size);
    setup_encoding(enc, window_size);

    // initialize factorizer
    lz77::LPFFactorizer lpf;
    lpf.min_reference_length(threshold);
    std::vector<lz77::Factor> factors;

    // initialize buffers
    auto block_offs = 0;
    auto block = std::make_unique<char[]>(window_size);

    while(begin != end) {
        // read next block
        Index block_num = 0;
        while(block_num < window_size && begin != end) {
            block[block_num++] = *begin++;
        }

        // compute the LZ77 factorization of the block
        factors.clear();
        lpf.factorize(block.get(), block.get() + block_num, std::back_inserter(factors));

        // encode the block
        Index curpos = 0;
        for(auto& f : factors) {
            // encode a LZ77 reference or a literal
            if(f.is_literal() || f.len == 1) {
                enc.write_uint(TOK_FACT_LEN, 1);
                enc.write_char(TOK_LITERAL, block[curpos]);

                ++num_literal;
            } else {
                auto const fpos = curpos;
                assert(fpos >= f.src);

                if(f.len >= MAX_LZ_REF_LEN) {
                    // encode the maximum length, then encode the rest as a special token
                    enc.write_uint(TOK_FACT_LEN, MAX_LZ_REF_LEN);
                    enc.write_uint(TOK_FACT_REMAINDER, f.len - MAX_LZ_REF_LEN);
                } else {
                    // simply encode the length
                    enc.write_uint(TOK_FACT_LEN, f.len);
                }

                // write source
                enc.write_uint(TOK_FACT_SRC, f.src);

                ++num_ref;
                longest = std::max(longest, (size_t)f.len);
                total_ref_len += f.len;
            }
            curpos += f.num_literals();
        }

        // advance
        block_offs += block_num;
    }
    enc.flush();

    // stats
    result.add("phrases_total", num_ref + num_literal);
    result.add("phrases_ref", num_ref);
    result.add("phrases_literal", num_literal);
    result.add("num_relevant", num_relevant);
    result.add("phrases_longest", longest);
    result.add("phrases_ref", num_ref);
    result.add("phrases_avg_ref_len", std::round(100.0 * ((double)total_ref_len / (double)num_ref)) / 100.0);
    enc.gather_stats(result);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void decompress_lz77_blockwise(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    auto const window_size = in.read(64);

    // initialize decoding
    BlockDecoder dec(in);
    setup_encoding(dec, window_size);

    auto block = std::make_unique<char[]>(window_size);
    auto block_offs = 0;
    size_t curpos = 0;

    while(in) {
        auto const len = dec.read_uint(TOK_FACT_LEN);
        size_t phrase_len;

        auto const gpos = block_offs + curpos;
        if(len == 1) {
            // a literal character
            auto const c = dec.read_char(TOK_LITERAL);
            block[curpos] = c;
            phrase_len = 1;
            if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": literal " << display(c) << std::endl;
        } else {
            phrase_len = len;

            // a block-local LZ77 reference
            if(len == MAX_LZ_REF_LEN) {
                // this factor may be even longer, decode remainder
                phrase_len += dec.read_uint(TOK_FACT_REMAINDER);
            }

            auto const src = dec.read_uint(TOK_FACT_SRC);
            assert(curpos >= src);
            auto const srcpos = curpos - src;
            for(size_t i = 0; i < phrase_len; i++) {
                block[curpos + i] = block[srcpos + i];
            }
            if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": lz (" << src << ", " << phrase_len << ")" << std::endl;
        }

        // advance
        curpos += phrase_len;

        if(curpos >= window_size) {
            // emit and advance to new block
            for(size_t i = 0; i < window_size; i++) {
                *out++ = block[i];
            }
            curpos = 0;
            block_offs += window_size;
        }
    }

    // emit final block
    for(size_t i = 0; i < curpos; i++) {
        *out++ = block[i];
    }
}
