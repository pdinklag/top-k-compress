#include "topk_common.hpp"

#include <trie_fcns.hpp>
#include <tdc/text/util.hpp>
#include <tdc/lz/lpf_factorizer.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'F') << 24 |
    ((uint64_t)'A') << 16 |
    ((uint64_t)'C') << 8 |
    ((uint64_t)'T');

using Index = uint32_t;
using Topk = TopKSubstrings<TopkTrieNode<>, true>;
using Node = Index;

constexpr bool PROTOCOL = false;

// nb: we use different token types to encode references
// the first token of any phrase is always the length:
// - a length of zero indicates a top-k trie reference
// - a length of one indicates a literal character
// - otherwise, we have a block-local LZ77 factor of the corresponding length
constexpr TokenType TOK_TRIE_REF = 0;
constexpr TokenType TOK_FACT_SRC = 1;
constexpr TokenType TOK_FACT_LEN = 2;
constexpr TokenType TOK_LITERAL = 3;

constexpr size_t MAX_LZ_REF_LEN = 255;

void setup_encoding(BlockEncodingBase& enc, size_t const k, size_t const window_size) {
    enc.params(TOK_TRIE_REF).encoding = TokenEncoding::Binary;
    enc.params(TOK_TRIE_REF).max = k - 1;
    enc.params(TOK_FACT_SRC).encoding = TokenEncoding::Binary;
    enc.params(TOK_FACT_SRC).max = window_size - 1;
    enc.params(TOK_FACT_LEN).encoding = TokenEncoding::Huffman;
    enc.params(TOK_LITERAL).encoding = TokenEncoding::Huffman;
}

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_fact(In begin, In const& end, Out out, size_t const threshold, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    // init stats
    size_t num_ref = 0;
    size_t num_frequent = 0;
    size_t num_literal = 0;
    Index longest = 0;
    size_t total_ref_len = 0;
    size_t num_relevant = 0;

    // write header and initialize encoding
    TopkHeader header(k, window_size, num_sketches, sketch_rows, sketch_columns);
    header.encode(out, MAGIC);

    BlockEncoder enc(out, 4, block_size);
    setup_encoding(enc, k, window_size);

    // initialize top-k
    Topk topk(k - 1, num_sketches, sketch_rows, sketch_columns);

    // initialize factorizer
    tdc::lz::LPFFactorizer<> lpf;
    lpf.min_reference_length(threshold);
    std::vector<tdc::lz::Factor> factors;

    // initialize buffers
    auto block_offs = 0;
    auto block = std::make_unique<char[]>(window_size);

    while(begin != end) {
        // read next block
        Index block_num = 0;
        while(block_num < window_size && begin != end) {
            block[block_num++] = *begin++;
        }

        auto topk_enter = [&](size_t const pos, size_t const len){
            ++num_relevant;

            if constexpr(PROTOCOL) std::cout << "enter: \"";
            Topk::StringState s = topk.empty_string();
            Node node;
            while(s.frequent && s.len < len && pos + s.len < block_num) {
                if constexpr(PROTOCOL) std::cout << display_inline(block[pos + s.len]);
                node = s.node;
                s = topk.extend(s, block[pos + s.len]);
            }
            if constexpr(PROTOCOL) std::cout << "\" (length " << s.len << " -> node " << node << ")" << std::endl;
        };

        // compute the LZ77 factorization of the block
        factors.clear();
        lpf.factorize(block.get(), block.get() + block_num, std::back_inserter(factors));

        // encode the block
        // at the beginning of each LZ77 factor, we attempt to find the longest possible string back in the top-k trie
        // if we find a string longer than the next LZ77 factor, we encode it using a trie reference and advance in the LZ77 factorization, potentially chopping
        // the factor that we reach into two fractions
        {
            Index z = 0; // the current LZ77 factor
            Index curpos = 0;
            while(curpos < block_num) {
                auto const gpos = block_offs + curpos;

                // find the longest string represented in the top-k trie starting at the current position
                Node v;
                Index dv;
                {
                    auto const& trie = topk.filter();
                    v = trie.root();
                    dv = 0;
                    while(curpos + dv < block_num) {
                        Node u;
                        if(trie.try_get_child(v, block[curpos + dv], u)) {
                            v = u;
                            ++dv;
                        } else {
                            break;
                        }
                    }
                }

                auto const& f = factors[z];
                if(dv >= f.num_literals()) {
                    // encode a top-k trie reference
                    assert(v > 0);

                    enc.write_uint(TOK_FACT_LEN, 0);
                    enc.write_uint(TOK_TRIE_REF, v);
                    ++num_frequent;

                    if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": top-k (" << v << ") / " << dv << std::endl;;

                    // advance in LZ77 factorization
                    if(curpos + dv < block_num) {
                        auto d = dv;
                        while(d >= factors[z].num_literals()) {
                            d -= factors[z].num_literals();
                            ++z;
                        }

                        if(d > 0) {
                            // chop current LZ77 factor
                            assert(factors[z].len > d);
                            factors[z].len -= d;
                        }
                    }

                    // enter
                    topk_enter(curpos, dv);
                    curpos += dv;
                } else {
                    // encode a LZ77 reference or a literal
                    if(f.is_literal() || f.num_literals() == 1) {
                        // a literal factor (possibly a reference of length one introduced due to chopping)
                        enc.write_uint(TOK_FACT_LEN, 1);
                        enc.write_char(TOK_LITERAL, block[curpos]);
                        if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": literal " << display(block[curpos]) << std::endl;
                        ++num_literal;

                        topk_enter(curpos, 1);
                        ++curpos;
                    } else {
                        // a real LZ77 reference
                        auto const fpos = curpos;
                        assert(fpos >= f.src);

                        // limit reference lengths, because we use them as a type indicator too
                        auto len = f.len;
                        while(len) {
                            auto limited_len = std::min(len, uintmax_t(MAX_LZ_REF_LEN));
                            enc.write_uint(TOK_FACT_LEN, limited_len);

                            if(limited_len == 1) {
                                // a length of 1 indicates a leftover literal character, so we should actually encode a literal

                                // nb: we may have encountered a run, and in that case fpos < src
                                // the modulo expression should handle this case properly
                                char const last = block[fpos - f.src + ((f.len - 1) % f.src)];
                                enc.write_char(TOK_LITERAL, last);
                                if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": literal " << display(last) << std::endl;
                            } else {
                                // write source
                                enc.write_uint(TOK_FACT_SRC, f.src);
                                if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": lz (" << f.src << ", " << limited_len << ")" << std::endl;
                            }

                            topk_enter(curpos, limited_len);
                            curpos += limited_len;

                            len -= limited_len;
                            ++num_ref;
                        }
                    }

                    // advance to next LZ77 factor
                    ++z;
                }
            }
        }
        block_offs += block_num;
    }
    enc.flush();

    // stats
    result.add("phrases_total", num_ref + num_literal + num_frequent);
    result.add("phrases_ref", num_ref);
    result.add("phrases_frequent", num_frequent);
    result.add("phrases_literal", num_literal);
    result.add("num_relevant", num_relevant);
    // result.add("phrases_longest", longest);
    // result.add("phrases_avg_ref_len", std::round(100.0 * ((double)total_ref_len / (double)num_ref)) / 100.0);
    // result.add("num_factors", num_factors);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_decompress_fact(In in, Out out) {
    // decode header
    TopkHeader header(in, MAGIC);
    auto const k = header.k;
    auto const window_size = header.window_size;    
    auto const num_sketches = header.num_sketches;
    auto const sketch_rows = header.sketch_rows;
    auto const sketch_columns = header.sketch_columns;

    // initialize decoding
    BlockDecoder dec(in, 4);
    setup_encoding(dec, k, window_size);
    Topk topk(k - 1, num_sketches, sketch_rows, sketch_columns);
    
    auto block = std::make_unique<char[]>(window_size);
    auto block_offs = 0;
    size_t curpos = 0;

    while(in) {
        auto const len = dec.read_uint(TOK_FACT_LEN);
        size_t phrase_len;

        auto const gpos = block_offs + curpos;
        if(len == 0) {
            // a top-k trie reference
            auto const node = dec.read_uint(TOK_TRIE_REF);
            phrase_len = topk.filter().spell(node, block.get() + curpos);
            if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": top-k (" << node << ") / " << phrase_len << std::endl;;
        } else if(len == 1) {
            // a literal character
            auto const c = dec.read_char(TOK_LITERAL);
            block[curpos] = c;
            phrase_len = 1;
            if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": literal " << display(c) << std::endl;
        } else {
            // a block-local LZ77 reference
            phrase_len = len;
            
            auto const src = dec.read_uint(TOK_FACT_SRC);
            assert(curpos >= src);
            auto const srcpos = curpos - src;
            for(size_t i = 0; i < len; i++) {
                block[curpos + i] = block[srcpos + i];
            }
            if constexpr(PROTOCOL) std::cout << "pos=" << gpos << ": lz (" << src << ", " << len << ")" << std::endl;
        }

        // enter string into top-k structure
        {
            if constexpr(PROTOCOL) std::cout << "enter: \"";
            Topk::StringState s = topk.empty_string();
            Node node;
            while(s.frequent && s.len < phrase_len) {
                assert(curpos + s.len < window_size);
                if constexpr(PROTOCOL) std::cout << display_inline(block[curpos + s.len]);
                node = s.node;
                s = topk.extend(s, block[curpos + s.len]);
            }
            if constexpr(PROTOCOL) std::cout << "\" (length " << s.len << " -> node " << node << ")" << std::endl;
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
