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

    PhraseBlockWriter writer(out, block_size, true, true);

    // initialize top-k
    using Topk = TopKSubstrings<TopkTrieNode<>, true>;
    Topk topk(k, num_sketches, sketch_rows, sketch_columns);

    // initialize factorizer
    tdc::lz::LPFFactorizer<> lpf;
    lpf.min_reference_length(threshold);
    std::vector<tdc::lz::Factor> factors;

    // initialize buffers
    auto block = std::make_unique<char[]>(window_size + 1);
    while(begin != end) {
        // read next block
        Index block_num = 0;
        while(block_num < window_size && begin != end) {
            block[block_num++] = *begin++;
        }
        block[block_num] = 0; // sentinel

        // compute the LZ77 factorization of the block
        factors.clear();
        lpf.factorize(block.get(), block.get() + block_num, std::back_inserter(factors));

        // encode the block
        // at the beginning of each LZ77 factor, we attempt to find the longest possible string back in the top-k trie
        // if we find a string longer than the next LZ77 factor, we encode it using a trie reference and advance in the LZ77 factorization, potentially chopping
        // the factor that we reach into two fractions
        {
            Index z = 0; // the current LZ77 factor
            Index pos = 0;
            while(pos < block_num) {
                auto const curpos = pos;

                // find the longest string represented in the top-k trie starting at the current position
                Node v;
                Index dv;
                {
                    auto const& trie = topk.filter();
                    v = trie.root();
                    dv = 0;
                    while(pos + dv < block_num) {
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
                Index flen;
                if(dv >= f.num_literals()) {
                    // encode a top-k trie reference
                    assert(v > 0);

                    flen = dv;
                    writer.write_ref(v << 1 + 1);
                    ++num_frequent;

                    // advance in LZ77 factorization
                    pos += dv;
                    if(pos < block_num) {
                        auto d = dv;
                        while(d >= factors[z].num_literals()) {
                            d -= factors[z].num_literals();
                            ++z;
                        }

                        if(d > 0) {
                            // chop current LZ77 factor
                            assert(factors[z].len > d);
                            factors[z].src += d;
                            factors[z].len -= d;
                        }
                    }
                } else {
                    // encode a LZ77 reference or a literal
                    flen = f.num_literals();

                    if(f.is_literal()) {
                        // a literal factor
                        assert(f.literal() == block[pos]);
                        writer.write_ref(0);
                        writer.write_literal(block[pos]);
                        ++num_literal;
                    } else if(f.num_literals() == 1) {
                        // a reference of length 1 may have been introduced due to chopping
                        // we convert it into a literal
                        assert(pos >= f.src);
                        assert(block[pos - f.src] == block[pos]);
                        writer.write_ref(0);
                        writer.write_literal(block[pos]);
                        ++num_literal;
                    } else {
                        // a real LZ77 reference
                        writer.write_ref(f.src << 1);
                        writer.write_len(f.len);
                        ++num_ref;
                    }

                    // advance to next LZ77 factor
                    pos += f.num_literals();
                    ++z;
                }

                // enter string beginning at current position
                {
                    ++num_relevant;
                    Topk::StringState s = topk.empty_string();
                    while(s.frequent && s.len < flen && curpos + s.len < block_num) {
                        s = topk.extend(s, block[curpos + s.len]);
                    }
                }
            }
        }
    }

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
    std::abort();
}
