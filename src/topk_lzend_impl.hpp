#include <tdc/code/concepts.hpp>

#include "topk_common.hpp"

#include <lzend_parsing.hpp>
#include <lzend_window_index.hpp>
#include <lzend_topk_parser.hpp>

#include <topk_lzend_trie.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'Z') << 24 |
    ((uint64_t)'E') << 16 |
    ((uint64_t)'N') << 8 |
    ((uint64_t)'D');

constexpr bool PROTOCOL = false;

using Index = uint32_t;

template<bool prefer_local, tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_lzend_compress(In begin, In const& end, Out out, size_t const max_block, size_t const k, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, pm::Result& result) {
    // initialize encoding
    TopkHeader header(k, max_block, num_sketches, sketch_rows, sketch_columns);
    header.encode(out, MAGIC);

    PhraseBlockWriter writer(out, block_size, true);

    // init stats
    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;

    // initialize parser
    using Trie = TopKLZEndTrie<Index>;
    using WindowIndex = LZEndWindowIndex<Index>;

    using Parser = LZEndTopkParser<prefer_local, Trie, WindowIndex, Index>;
    
    // parse
    Trie trie(k, num_sketches, sketch_rows, sketch_columns);
    Parser parser(max_block, trie);

    size_t i = 0;
    auto emit = [&](Parser::Phrase const& phrase) {
        if constexpr(PROTOCOL) {
            std::cout << "phrase #" << (num_phrases+1) << ": i=" << i << ", (" << phrase.link;
            if(parser.is_trie_ref(phrase.link)) {
                std::cout << "*";
            }
            std::cout << ", " << phrase.len << ", " << display(phrase.last) << ")" << std::endl;
        }
        
        i += phrase.len;

        ++num_phrases;
        if(phrase.len > 1) {
            // referencing phrase
            writer.write_ref(phrase.link);
            writer.write_len(parser.is_trie_ref(phrase.link) ? 0 : phrase.len - 1); // 
            writer.write_literal(phrase.last);

            ++num_ref;
        } else {
            // literal phrase
            ++num_literal;
            writer.write_ref(0);
            writer.write_literal(phrase.last);
        }
        
        longest = std::max(longest, size_t(phrase.len));
        total_len += phrase.len;
        furthest = std::max(furthest, size_t(phrase.link));
        total_ref += phrase.link;
    };

    parser.on_emit_phrase = emit;
    parser.parse(begin, end);

    // flush writer
    writer.flush();

    // get parser stats
    auto const parser_stats = parser.stats();

    result.add("phrases_total", num_phrases);
    result.add("phrases_ref", num_ref);
    result.add("phrases_literal", num_literal);
    result.add("phrases_longest", longest);
    result.add("phrases_furthest", furthest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("phrases_avg_dist", std::round(100.0 * ((double)total_ref / (double)num_phrases)) / 100.0);
    result.add("phrases_from_trie", parser_stats.phrases_from_trie);
    result.add("phrases_from_trie_avg_len", std::round(100.0 * ((double)parser_stats.phrases_from_trie_total_len / (double)parser_stats.phrases_from_trie)) / 100.0);
    result.add("prefer_local", prefer_local);
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void topk_lzend_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }
    
    std::cerr << "not yet implemented" << std::endl;
    std::abort();
}
