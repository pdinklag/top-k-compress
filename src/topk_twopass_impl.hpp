#include <iopp/file_input_stream.hpp>
#include <iopp/file_output_stream.hpp>
#include <block_coding.hpp>

#include <pm/result.hpp>

#include <topk_prefixes_misra_gries.hpp>

using Node = uint32_t;
using Index = uint32_t;
using Topk = TopKPrefixesMisraGries<Node>;

constexpr TokenType TOK_TRIE_REF = 0;
constexpr TokenType TOK_LITERAL = 1;

void setup_encoding(BlockEncodingBase& enc, size_t const k) {
    enc.register_binary(k-1); // TOK_TRIE_REF
    enc.register_binary(255, false);   // TOK_LITERAL
}

template<typename Trie, iopp::BitSink Out>
void encode_topology(Trie const& trie, Node const v, Out& out) {
    // balanced parantheses
    out.write(bool(1));
    
    auto const& children = trie.node(v).children;
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
void topk_compress_twopass(iopp::FileInputStream& in, Out out, size_t const k, size_t const max_frequency, size_t const block_size, pm::Result& result) {
    // pass 1: build top-k LZ78 trie
    Topk::TrieType trie;
    {
        auto begin = in.begin();
        auto const end = in.end();

        Topk topk(k - 1, max_frequency);
        auto s = topk.empty_string();
        while(begin != end) {
            // read next character
            auto const c = *begin++;
            auto next = topk.extend(s, c);
            s = next.frequent ? next : topk.empty_string();
        }

        trie = std::move(topk.trie());
    }

    // pass 2: parse

    // stats
    size_t num_literal = 0;
    size_t num_trie = 0;
    size_t longest = 0;
    size_t total_len = 0;
    size_t size_trie_topology = 0;
    size_t size_trie_labels = 0;

    // initialize encoding
    BlockEncoder enc(out, block_size);
    setup_encoding(enc, k);

    // encode trie
    {
        // topology
        auto bits0 = out.num_bits_written();
        encode_topology(trie, trie.root(), out);
        size_trie_topology = (out.num_bits_written() - bits0) / 8;

        // labels
        std::string labels;
        labels.reserve(k);
        for(Index i = 1; i < trie.size(); i++) {
            labels.push_back(trie.node(i).inlabel);
        }

        bits0 = out.num_bits_written();
        code::HuffmanTree<char> huff(labels.begin(), labels.end());
        huff.encode(out);

        auto table = huff.table();
        for(auto const c : labels) {
            code::Huffman::encode(out, uint8_t(c), table);
        }
        size_trie_labels = (out.num_bits_written() - bits0) / 8;
        
        out.flush();
    }

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

    auto const num_phrases = num_literal + num_trie;
    result.add("phrases_total", num_literal + num_trie);
    result.add("phrases_literal", num_literal);
    result.add("phrases_trie", num_trie);
    result.add("phrases_longest", longest);
    result.add("phrases_avg_len", std::round(100.0 * ((double)total_len / (double)num_phrases)) / 100.0);
    result.add("size_trie_topology", size_trie_topology);
    result.add("size_trie_labels", size_trie_labels);
    result.add("size_trie", size_trie_topology + size_trie_labels);
}
