#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>
#include <block_coding.hpp>

#include <pm/result.hpp>

#include <code/counter.hpp>

#include <topk_prefixes_misra_gries.hpp>
#include <simple_trie.hpp>
#include <small_trie.hpp>

#include <stack>
#include <unordered_map>
#include <vector>

namespace topk_twopass {

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'2') << 24 |
    ((uint64_t)'P') << 16 |
    ((uint64_t)'S') << 8 |
    ((uint64_t)'S');

using Node = uint32_t;
using Index = uint32_t;
using Topk = TopKPrefixesMisraGries<Node>;

constexpr TokenType TOK_TRIE_REF = 0;
constexpr TokenType TOK_LITERAL = 1;

void setup_encoding(BlockEncodingBase& enc, size_t const k) {
    enc.register_binary(k-1); // TOK_TRIE_REF
    enc.register_binary(255, false);   // TOK_LITERAL
}

template<typename Trie>
void gather_labels_preorder(Trie const& trie, Node const v, std::string& labels) {
    labels.push_back(trie.node(v).inlabel);

    auto const& children = trie.node(v).children;
    for(size_t i = 0; i < children.size(); i++) {
        gather_labels_preorder(trie, children[i], labels);
    }
}

template<typename Trie>
void gather_labels(Trie const& trie, Node const v, std::string& labels) {
    auto const& children = trie.children_of(v);
    for(size_t i = 0; i < children.size(); i++) { 
        labels.push_back(children.label(i));
    }
    for(size_t i = 0; i < children.size(); i++) {
        gather_labels(trie, children[i], labels);
    }
}

template<typename Trie, iopp::BitSink Out>
void encode_topology(Trie const& trie, Node const v, Out& out) {
    // balanced parantheses
    out.write(bool(1));
    
    auto const& children = trie.children_of(v);
    for(size_t i = 0; i < children.size(); i++) {
        encode_topology(trie, children[i], out);
    }

    out.write(bool(0));
}

inline void write_file_str(std::filesystem::path const& path, std::string const& s) {
    iopp::FileOutputStream fout(path);
    std::copy(s.begin(), s.end(), iopp::StreamOutputIterator(fout));
}

template<iopp::BitSink Out>
void compress(iopp::FileInputStream& in, Out out, size_t const k, size_t const max_freq, size_t const block_size, pm::Result& result) {
    // write header and initialize encoding
    out.write(MAGIC, 64);
    out.write(k, 64);
    out.write(max_freq, 64);

    // pass 1: build top-k LZ78 trie
    pm::Stopwatch sw;
    sw.start();

    using ReducedTrie = SmallTrie; //SimpleTrie<Node>;
    ReducedTrie trie;
    {
        auto begin = in.begin();
        auto const end = in.end();

        Topk topk(k - 1, max_freq);
        auto s = topk.empty_string();
        while(begin != end) {
            // read next character
            auto const c = *begin++;
            auto next = topk.extend(s, c);
            s = next.frequent ? next : topk.empty_string();
        }

        // reduce top-k trie to a simple ("static") trie, depth-first
        auto topk_trie = std::move(topk.trie());
        trie = ReducedTrie(topk_trie);
        // reduce_depth_first(topk_trie, topk_trie.root(), trie, trie.root());
    }
    sw.stop();
    result.add("time_build", (size_t)sw.elapsed_time_millis());

    {
        auto const trie_mem = trie.mem_size();
        result.add("trie_mem", trie.mem_size());
        result.add("trie_mem_avg_per_node", std::round(100.0 * ((double)trie_mem / (double)k)) / 100.0);
    }

    // relabel and encode trie
    sw.start();
    {
        // topology
        auto bits0 = out.num_bits_written();
        encode_topology(trie, trie.root(), out);
        auto const size_trie_topology = (out.num_bits_written() - bits0) / 8;

        // compress labels using Huffman
        {
            std::string labels;
            labels.reserve(k);
            {
                auto const& root_children = trie.children_of(trie.root());
                for(size_t i = 0; i < root_children.size(); i++) {
                    gather_labels(trie, root_children[i], labels);
                }
            }

            bits0 = out.num_bits_written();
            code::HuffmanTree<char> huff(labels.begin(), labels.end());
            huff.encode(out);

            auto table = huff.table();
            for(auto const c : labels) {
                code::Huffman::encode(out, uint8_t(c), table);
            }
        }
        
        auto const size_trie_labels = (out.num_bits_written() - bits0) / 8;
        auto const size_trie = size_trie_topology + size_trie_labels;

        result.add("outsize_trie_topology", size_trie_topology);
        result.add("outsize_trie_labels", size_trie_labels);
        result.add("outsize_trie", size_trie);

        // debug
        trie.print_debug_info();

        out.flush();
    }
    sw.stop();
    result.add("time_enc_trie", (size_t)sw.elapsed_time_millis());

    // pass 2: parse

    // stats
    size_t num_literal = 0;
    size_t num_trie = 0;
    size_t longest = 0;
    size_t total_len = 0;

    // initialize encoding
    BlockEncoder enc(out, block_size);
    setup_encoding(enc, k);

    sw.start();
    {
        in.seekg(0, std::ios::beg);

        auto begin = in.begin();
        auto const end = in.end();

        auto const root = trie.root();
        auto v = root;
        size_t dv = 0;

        while(begin != end) {
            auto const c = *begin++;

            Node u;
            if(trie.try_get_child(v, c, u)) {
                // cool, go on
                v = u;
                ++dv;
            } else {
                if(v != root) {
                    enc.write_uint(TOK_TRIE_REF, v);

                    total_len += dv;
                    longest = std::max(longest, dv);
                    ++num_trie;

                    // try to find a child of the root for label c
                    if(trie.try_get_child(root, c, v)) {
                        dv = 1;
                    } else {
                        // none found, return to root
                        v = root;
                        dv = 0;
                    }
                }

                // if v is the root, encode (0, c)
                if(v == root) {
                    enc.write_uint(TOK_TRIE_REF, 0);
                    enc.write_uint(TOK_LITERAL, c);
                    ++num_literal;
                }
            }
        }

        if(v != root) {
            enc.write_uint(TOK_TRIE_REF, v);

            total_len += dv;
            longest = std::max(longest, dv);
            ++num_trie;
        }

        enc.flush();
    }
    sw.stop();
    result.add("time_parse", (size_t)sw.elapsed_time_millis());

    auto const num_phrases = num_literal + num_trie;
    result.add("phrases_total", num_literal + num_trie);
    result.add("phrases_literal", num_literal);
    result.add("phrases_trie", num_trie);
    result.add("phrases_longest", longest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
}

}
