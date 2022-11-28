#include <vector>

#include <tdc/code/concepts.hpp>
#include <tdc/util/concepts.hpp>

#include <phrase_block_writer.hpp>
#include <phrase_block_reader.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'7') << 40 |
    ((uint64_t)'8') << 32 |
    ((uint64_t)'F') << 24 |
    ((uint64_t)'U') << 16 |
    ((uint64_t)'L') << 8 |
    ((uint64_t)'L');

class TrieFCNS {
public:
    using Node = size_t;

private:
    std::vector<Node> fc_;
    std::vector<Node> ns_;
    std::vector<char> in_;

    bool try_get_child(Node const v, char const c, Node& out_child) {
        auto x = fc_[v];
        if(x) {
            if(in_[x] == c) {
                out_child = x;
                return true;
            } else {
                while(x) {
                    x = ns_[x];
                    if(x && in_[x] == c) {
                        out_child = x;
                        return true;
                    }
                }
            }
        }
        return false;
    }

    Node insert_child(Node const parent, char const c) {
        auto const v = size();

        fc_.push_back(0);
        ns_.push_back(fc_[parent]);
        in_.push_back(c);

        fc_[parent] = v;
        return v;
    }

public:
    TrieFCNS() {
        // create root
        fc_.push_back(0);
        ns_.push_back(0);
        in_.push_back(0);
    }

    Node root() const { return 0; }

    size_t size() const { return fc_.size(); }

    bool follow_edge(Node const v, char const c, Node& out_node) {
        auto const found = try_get_child(v, c, out_node);
        if(!found) {
            out_node = insert_child(v, c);
        }
        return found;
    }
};

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void lz78_compress(In begin, In const& end, Out out, size_t const block_size, pm::Result& result) {
    out.write(MAGIC, 64);

    PhraseBlockWriter writer(out, block_size);

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
            writer.write_ref(u);
            writer.write_literal(c);

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
        writer.write_ref(u);
        ++num_phrases; // final phrase
    }

    writer.flush();

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

    std::string dec; // yes, we do it in RAM...
    std::vector<std::pair<size_t, char>> factors;
    factors.emplace_back(0, 0); // align

    std::function<void(size_t const)> decode;
    decode = [&](size_t const f){
        if(f) {
            decode(factors[f].first);
            dec.push_back(factors[f].second);
        }
    };

    PhraseBlockReader reader(in);
    while(in) {
        auto const f = reader.read_ref();
        decode(f);

        if(in) {
            auto const c = reader.read_literal();
            dec.push_back(c);
            factors.emplace_back(f, c);
        }
    }

    // output
    std::copy(dec.begin(), dec.end(), out);
}
