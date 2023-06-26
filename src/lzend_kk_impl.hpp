#include <cassert>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <tdc/code/concepts.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

#include <lzend_parsing.hpp>
#include <lzend_rev_phrase_trie.hpp>
#include <lzend_window_index.hpp>
#include <lzend_kk_parser.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'E') << 40 |
    ((uint64_t)'N') << 32 |
    ((uint64_t)'D') << 24 |
    ((uint64_t)'_') << 16 |
    ((uint64_t)'K') << 8 |
    ((uint64_t)'K');

constexpr bool PROTOCOL = false;

using Index = uint32_t;

template<bool prefer_local, tdc::InputIterator<char> In, iopp::BitSink Out>
void lzend_kk_compress(In begin, In const& end, Out out, size_t const max_block, size_t const block_size, pm::Result& result) {
    using Parsing = LZEndParsing<char, Index>;
    using Trie = LZEndRevPhraseTrie<char, Index>;
    using WindowIndex = LZEndWindowIndex<Index>;

    using Parser = LZEndKKParser<prefer_local, Parsing, Trie, WindowIndex, Index>;

    // parse
    Parser parser(max_block);
    parser.parse(begin, end);

    // get parsing stats
    auto const state_mem = parser.memory_profile();
    auto const trie_mem = parser.trie().memory_profile();
    auto const trie_stats = parser.trie().stats();
    auto const trie_nodes = parser.trie().size();
    auto const parser_stats = parser.stats();

    // get parsing
    auto const z = parser.num_phrases();
    auto const& parsing = parser.parsing();

    // init stats
    size_t num_phrases = 0;
    size_t num_ref = 0;
    size_t num_literal = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t furthest = 0;
    size_t total_ref = 0;

    // initialize encoding
    out.write(MAGIC, 64);
    PhraseBlockWriter writer(out, block_size, true);

    // write phrases
    {
        size_t i = 0;
        for(size_t j = 1; j <= z; j++) {
            if constexpr(PROTOCOL) {
                std::cout << "phrase #" << j << ": i=" << i << ", (" << parsing[j].link << ", " << parsing[j].len << ", " << display(parsing[j].last) << std::endl;
            }
            
            i += parsing[j].len;

            ++num_phrases;
            if(parsing[j].len > 1) {
                // referencing phrase
                writer.write_ref(parsing[j].link);
                writer.write_len(parsing[j].len - 1);
                writer.write_literal(parsing[j].last);

                ++num_ref;

            } else {
                // literal phrase
                ++num_literal;
                writer.write_ref(0);
                writer.write_literal(parsing[j].last);
            }
            
            longest = std::max(longest, size_t(parsing[j].len));
            total_len += parsing[j].len;
            furthest = std::max(furthest, size_t(parsing[j].link));
            total_ref += parsing[j].link;
        }

        if constexpr(PROTOCOL) std::cout << std::endl;
    }

    // flush
    writer.flush();

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
    result.add("trie_nodes", trie_nodes);
    result.add("trie_num_match_extract", trie_stats.num_match_extract);
    result.add("trie_num_recalc", trie_stats.num_recalc);
    result.add("trie_longest_lcs", trie_stats.longest_lcs);
    result.add("mem_glob_buffer", state_mem.buffer);
    result.add("mem_glob_lnks_lens", state_mem.lnks_lens);
    result.add("mem_glob_parsing", state_mem.parsing);
    result.add("mem_glob_phrase_hashes", state_mem.phrase_hashes);
    result.add("mem_glob", state_mem.total());
    result.add("mem_trie", trie_mem.total());
    result.add("mem_trie_nodes", trie_mem.nodes);
    result.add("mem_trie_phrase_ptrs", trie_mem.phrase_nodes);
    result.add("mem_trie_nav", trie_mem.nav);
    result.add("mem_trie_map", trie_mem.map);

    auto const& win_mem = parser_stats.max_window_memory;
    result.add("mem_window_rev_string", win_mem.reverse_window);
    result.add("mem_window_lcp_isa", win_mem.lcp_isa);
    result.add("mem_window_tmp_sa", win_mem.tmp_sa);
    result.add("mem_window_marked", win_mem.marked);
    result.add("mem_window_fingerprints", win_mem.fingerprints);
    result.add("mem_window_rmq", win_mem.rmq);
    result.add("mem_window", win_mem.total());
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void lzend_kk_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }
    
    std::string dec;
    std::vector<size_t> factors;
    
    PhraseBlockReader reader(in, true);
    while(in) {
        auto const q = reader.read_ref();
        auto const len = (q > 0) ? reader.read_len() : 0;

        if(len > 0) {
            auto p = factors[q-1] + 1 - len;
            for(size_t i = 0; i < len; i++) {
                dec.push_back(dec[p++]);
            }
        }
        
        if(in) {
            auto const c = reader.read_literal();
            factors.push_back(dec.length());
            dec.push_back(c);

            if constexpr(PROTOCOL) {
                std::cout << "factor #" << factors.size() << ": i=" << (dec.size() - len - 1) << ", (" << q << ", " << len << ", " << display(c) << ")" << std::endl;
            }
        } else {
            if constexpr(PROTOCOL) {
                std::cout << "factor #" << factors.size() << ": i=" << (dec.size() - len - 1) << ", (" << q << ", " << len << ", <EOF>)" << std::endl;
            }
        }
    }

    // output
    std::copy(dec.begin(), dec.end(), out);
}
