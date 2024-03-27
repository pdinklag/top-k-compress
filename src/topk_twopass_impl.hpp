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
void gather_labels(Trie const& trie, Node const v, char const inlabel, std::string& labels) {
    labels.push_back(inlabel);

    auto const& children = trie.children_of(v);
    for(size_t i = 0; i < children.size(); i++) { 
        gather_labels(trie, children[i], children.label(i), labels);
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

template<iopp::InputIterator<char> In>
auto compute_topk(In begin, In const end, size_t const k, size_t const max_freq) {
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
    return topk_trie;
}

struct Phrase {
    uintmax_t node;
    uintmax_t len;
    char literal;

    Phrase(uintmax_t const ref, uintmax_t len) : node(ref), len(len), literal(0) {
    }

    Phrase(char const literal) : node(0), len(1), literal(literal) {
    }

    bool is_literal() const {
        return node == 0;
    }
};

template<iopp::InputIterator<char> In, typename Trie>
void parse(In begin, In const end, Trie const& trie, std::function<void(Phrase)> emit) {
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
                emit(Phrase(v, dv));

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
                emit(Phrase(c));
            }
        }
    }

    if(v != root) {
        emit(Phrase(v, dv));
    }
}

template<iopp::BitSink Out>
void compress(iopp::FileInputStream& in, Out out, size_t const k, size_t const max_freq, size_t const block_size, pm::Result& result) {
    // write header and initialize encoding
    out.write(MAGIC, 64);
    out.write(k, 64);

    // pass 1: build top-k LZ78 trie
    pm::Stopwatch sw;
    sw.start();

    using ReducedTrie = SmallTrie<false>; // SimpleTrie<Node>;
    ReducedTrie trie(compute_topk(in.begin(), in.end(), k, max_freq));

    sw.stop();
    result.add("time_build", (size_t)sw.elapsed_time_millis());

    {
        auto const trie_mem = trie.mem_size();
        result.add("trie_mem", trie.mem_size());
        result.add("trie_mem_avg_per_node", std::round(100.0 * ((double)trie_mem / (double)k)) / 100.0);
    }

    // encode trie
    sw.start();
    {
        // topology
        auto bits0 = out.num_bits_written();
        encode_topology(trie, trie.root(), out);
        auto const size_trie_topology = (out.num_bits_written() - bits0) / 8;

        // compress labels using Huffman
        bits0 = out.num_bits_written();
        {
            std::string labels;
            labels.reserve(k-1);
            gather_labels(trie, trie.root(), 0, labels);

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

        // out.flush(); // nb: must not flush, there is no corresponding function in the bit source yet
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

        parse(in.begin(), in.end(), trie, [&](Phrase f){
            if(f.is_literal()) {
                enc.write_uint(TOK_TRIE_REF, 0);
                enc.write_uint(TOK_LITERAL, f.literal);
                ++num_literal;
            } else {
                enc.write_uint(TOK_TRIE_REF, f.node);
                total_len += f.len;
                longest = std::max(longest, f.len);
                ++num_trie;
            }
        });

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

template<iopp::BitSource In, std::output_iterator<char> Out>
void decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    auto const k = in.read(64);
    size_t num_nodes = 1;

    // decode trie
    using ReducedTrie = SmallTrie<true>; // SimpleTrie<Node>
    ReducedTrie trie;
    {
        // topology
        std::vector<bool> topology;
        topology.reserve(2 * k);
        {
            const bool open_root = in.read();
            assert(open_root == true); // nb: must at least open the root ...

            topology.push_back(open_root);

            size_t dv = 1;
            while(dv) {
                const bool b = in.read();
                if(b) {
                    // open node
                    ++dv;
                    ++num_nodes;
                } else {
                    // close node
                    --dv;
                }
                topology.push_back(b);
            }
        }

        assert(topology.size() % 2 == 0);
        assert(topology.size() / 2 == num_nodes);
        assert(topology.size() / 2 <= k);

        // labels
        std::string labels;
        labels.reserve(num_nodes);
        {
            code::HuffmanTree<char> huff(in);
            for(size_t i = 0; i < num_nodes; i++) {
                labels.push_back(code::Huffman::decode(in, huff.root()));
            }
        }

        // reconstruct trie from topology and labels
        SimpleTrie<Node> simple_trie(topology, labels);
        trie = ReducedTrie(simple_trie);
    }

    {
        auto const trie_mem = trie.mem_size();
        std::cout << "# trie_mem=" << trie_mem
            << ", trie_mem_avg_per_node=" << std::round(100.0 * ((double)trie_mem / (double)k)) / 100.0 << std::endl;
    }

    // decode input
    BlockDecoder dec(in);
    setup_encoding(dec, k);
    auto buffer = std::make_unique<char[]>(k);

    while(in) {
        auto const v = dec.read_uint(TOK_TRIE_REF);
        if(v == 0) {
            // literal
            *out++ = (char)dec.read_uint(TOK_LITERAL);
        } else {
            // trie
            auto const len = trie.spell(v, buffer.get());
            std::copy(buffer.get(), buffer.get() + len, out);
        }
    }
}

}
