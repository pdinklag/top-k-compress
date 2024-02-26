#include <display.hpp>

#include <lz77/lpf_factorizer.hpp>

#include <block_coding.hpp>
#include <pm/result.hpp>

#include <omp.h>
#include <valgrind.hpp>

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
constexpr TokenType TOK_FACT_REMAINDER = 4;

constexpr size_t MAX_LZ_REF_LEN = 255;

void setup_encoding(BlockEncodingBase& enc, size_t const k, size_t const window_size) {
    enc.register_binary(k-1);           // TOK_TRIE_REF
    enc.register_binary(window_size-1); // TOK_FACT_SRC
    enc.register_huffman();             // TOK_FACT_LEN
    enc.register_binary(255, false);    // TOK_FACT_LITERAL
    enc.register_binary(window_size, false); // TOK_FACT_REMAINDER
}

template<typename Topk, iopp::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_plz77(In begin, In const& end, Out out, size_t const threshold, size_t const k, size_t const window_size, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    // init stats
    size_t num_lz = 0;
    size_t num_trie = 0;
    size_t num_literal = 0;
    size_t trie_longest = 0;
    size_t lz_longest = 0;
    size_t total_trie_len = 0;
    size_t total_lz_len = 0;
    size_t num_relevant = 0;

    // write header and initialize encoding
    out.write(MAGIC, 64);
    out.write(k, 64);
    out.write(window_size, 64);
    out.write(sketch_columns, 64);

    BlockEncoder enc(out, block_size);
    setup_encoding(enc, k, window_size);

    // initialize top-k
    auto const num_threads = omp_get_max_threads();
    auto topk = std::make_unique<Topk[]>(num_threads);
    for(size_t i = 0; i < num_threads; i++) {
        topk[i] = Topk(k / num_threads, sketch_columns / num_threads);
    }

    // initialize factorizer
    lz77::LPFFactorizer lpf;
    lpf.min_reference_length(threshold);
    auto factors = std::make_unique<std::vector<lz77::Factor>[]>(num_threads);

    // initialize phrase buffers
    struct Phrase {
        Index len;
        Index ref;
        Index depth;
    } __attribute__((packed));
    auto phrases = std::make_unique<std::vector<Phrase>[]>(num_threads);

    // initialize buffers
    auto block = std::make_unique<char[]>(window_size);

    while(begin != end) {
        // read next block
        Index block_num = 0;
        while(block_num < window_size && begin != end) {
            block[block_num++] = *begin++;
        }

        // process block in parallel
        Index const num_per_thread = block_num / num_threads;
        #pragma omp parallel
        {
            auto const thread = omp_get_thread_num();
            auto const local_block_offs = thread * num_per_thread;
            auto const local_block_end = std::min(local_block_offs + num_per_thread, block_num);

            // compute the LZ77 factorization of the block
            factors[thread].clear();
            lpf.factorize(block.get() + local_block_offs, block.get() + local_block_end, std::back_inserter(factors[thread]));

            // compute phrases
            auto topk_enter = [&](size_t const pos, size_t const len){
                typename Topk::StringState s = topk[thread].empty_string();
                Node node;
                while(s.frequent && s.len < len && pos + s.len < local_block_end) {
                    node = s.node;
                    s = topk[thread].extend(s, block[pos + s.len]);
                }
            };

            {
                Index z = 0; // the current LZ77 factor
                Index curpos = local_block_offs;
                while(curpos < local_block_end) {
                    // find the longest string represented in the top-k trie starting at the current position
                    Node v;
                    Index dv = topk[thread].find(block.get() + curpos, local_block_end - curpos, v);

                    auto const& f = factors[thread][z];
                    if(dv >= f.num_literals()) {
                        // encode a top-k trie reference
                        assert(v > 0);

                        phrases[thread].push_back(Phrase{0, v, dv});

                        // advance in LZ77 factorization
                        if(curpos + dv < block_num) {
                            auto d = dv;
                            while(d >= factors[thread][z].num_literals()) {
                                d -= factors[thread][z].num_literals();
                                ++z;
                            }

                            if(d > 0) {
                                // chop current LZ77 factor
                                assert(factors[thread][z].len > d);
                                factors[thread][z].len -= d;
                            }
                        }

                        // enter
                        topk_enter(curpos, dv);
                        curpos += dv;
                    } else {
                        // encode a LZ77 reference or a literal
                        if(f.is_literal() || f.num_literals() == 1) {
                            // a literal factor (possibly a reference of length one introduced due to chopping)
                            phrases[thread].push_back(Phrase{1, (Index)block[curpos]});

                            topk_enter(curpos, 1);
                            ++curpos;
                        } else {
                            // a real LZ77 reference
                            auto const fpos = curpos;
                            assert(fpos >= f.src);

                            phrases[thread].push_back(Phrase{(Index)f.len, (Index)f.src});

                            // enter
                            topk_enter(curpos, f.len);
                            curpos += f.len;
                        }

                        // advance to next LZ77 factor
                        ++z;
                    }
                }
            }
        }

        // encode phrases
        for(size_t p = 0; p < num_threads; p++) {
            for(auto phrase : phrases[p]) {
                if(phrase.len == 0) {
                    // top-k phrase
                    enc.write_uint(TOK_FACT_LEN, 0);
                    enc.write_uint(TOK_TRIE_REF, phrase.ref);

                    ++num_trie;
                    trie_longest = std::max(trie_longest, (size_t)phrase.depth);
                    total_trie_len += phrase.depth;
                } else if(phrase.len == 1) {
                    // literal phrase
                    enc.write_uint(TOK_FACT_LEN, 1);
                    enc.write_char(TOK_LITERAL, (char)phrase.ref);

                    ++num_literal;
                } else {
                    // rferencing phrase
                    if(phrase.len >= MAX_LZ_REF_LEN) {
                        // encode the maximum length, then encode the rest as a special token
                        enc.write_uint(TOK_FACT_LEN, MAX_LZ_REF_LEN);
                        enc.write_uint(TOK_FACT_REMAINDER, phrase.len - MAX_LZ_REF_LEN);
                    } else {
                        // simply encode the length
                        enc.write_uint(TOK_FACT_LEN, phrase.len);
                    }

                    // write source
                    enc.write_uint(TOK_FACT_SRC, phrase.ref);

                    ++num_lz;
                    lz_longest = std::max(trie_longest, (size_t)phrase.len);
                    total_lz_len += phrase.len;
                }
            }
            phrases[p].clear();
        }

        // TODO: merge tries
    }
    enc.flush();

    // stats
    topk[0].print_debug_info();
    result.add("num_threads", num_threads);
    result.add("phrases_total", num_lz + num_literal + num_trie);
    result.add("phrases_ref", num_lz + num_trie);
    result.add("phrases_literal", num_literal);
    // result.add("num_relevant", num_relevant);
    result.add("phrases_longest", std::max(trie_longest, lz_longest));
    result.add("phrases_longest_lz", lz_longest);
    result.add("phrases_longest_trie", trie_longest);
    result.add("phrases_ref_lz", num_lz);
    result.add("phrases_ref_trie", num_trie);
    result.add("phrases_avg_ref_len", std::round(100.0 * ((double)(total_lz_len + total_trie_len) / (double)(num_lz + num_trie))) / 100.0);
    result.add("phrases_avg_ref_len_lz", std::round(100.0 * ((double)total_lz_len / (double)num_lz)) / 100.0);
    result.add("phrases_avg_ref_len_trie", std::round(100.0 * ((double)total_trie_len / (double)num_trie)) / 100.0);
}
