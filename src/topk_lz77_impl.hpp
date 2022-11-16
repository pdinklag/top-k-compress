#include "topk_common.hpp"

#include <tlx/container/ring_buffer.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'L') << 56 |
    ((uint64_t)'Z') << 48 |
    ((uint64_t)'7') << 40 |
    ((uint64_t)'7') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'I') << 16 |
    ((uint64_t)'K') << 8 |
    ((uint64_t)'E');

constexpr bool PROTOCOL = false;
constexpr bool DEBUG = false;

struct TopkLZ77TrieNode : public TopkTrieNode<> {
    using TopkTrieNode::Character;
    using TopkTrieNode::Index;

    static constexpr Index DOES_NOT_EXIST = -1;

    using WeinerLinkArray = TrieEdgeArray<Character, Index, TrieNode::Size>;

    Index           suffix_link;
    WeinerLinkArray weiner_links;
    Character       root_label;
    size_t          prev_occ;

    TopkLZ77TrieNode(Index const _parent, Character const _inlabel) : TopkTrieNode(_parent, _inlabel), suffix_link(DOES_NOT_EXIST) {
    }
    
    TopkLZ77TrieNode() : TopkLZ77TrieNode(0, 0) {
    }

} __attribute__((packed));

template<bool fast, tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_lz77(In begin, In const& end, Out out, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size, size_t const threshold = 2) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    out.write(MAGIC, 64);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    using Topk = TopKSubstrings<TopkLZ77TrieNode>;
    using FilterIndex = TopkLZ77TrieNode::Index;
    static constexpr FilterIndex DOES_NOT_EXIST = TopkLZ77TrieNode::DOES_NOT_EXIST;

    Topk topk(k, num_sketches, sketch_rows, sketch_columns);
    size_t n = 0;
    size_t num_phrases = 0;
    size_t num_literal = 0;
    size_t num_ref = 0;
    size_t total_ref_len = 0;
    size_t total_ref_dist = 0;
    size_t longest = 0;

    // callbacks
    topk.on_filter_node_inserted = [&](FilterIndex const v){
        // a new node has been inserted, we have to find its suffix link and Weiner links and update the trie accordingly

        assert(v); // cannot be the root
        auto& vdata = topk.filter_node(v);
        auto const c = vdata.inlabel;

        auto const parent = vdata.parent;
        auto& parent_data = topk.filter_node(parent);
        if(!parent) {
            // the parent is the root
            // in the root, the set of Weiner links equals the set of children
            vdata.root_label = c;

            // make v the root's Weiner link for label c
            parent_data.weiner_links.insert(c, v);

            // make root the suffix link for v
            vdata.suffix_link = parent;
        } else {
            // the parent is an inner node other than the root
            vdata.root_label = parent_data.root_label;

            // STEP 1: find v's suffix link
            // for this, follow the parent's suffix link and test if it has a child labeled c
            // if that is the case, then it is the suffix link of v and v is its Weiner link for v's root label

            // follow the parent's suffix link if it exists
            auto u = parent_data.suffix_link;
            if(u == DOES_NOT_EXIST) {
                // the parent's suffix link does not exist
                // this means that v cannot have a suffix link
                vdata.suffix_link = DOES_NOT_EXIST;
            } else {
                // if u has a child labeled c, it is the suffix link of v
                auto& udata = topk.filter_node(u);

                FilterIndex link;
                if(udata.children.try_get(c, link)) {
                    // suffix link exists, its Weiner link for v's root label is v
                    vdata.suffix_link = link;
                    topk.filter_node(vdata.suffix_link).weiner_links.insert(vdata.root_label, v);
                } else {
                    // suffix link does not exist
                    vdata.suffix_link = DOES_NOT_EXIST;
                }
            }

            // STEP 2: find v's Weiner links
            // for this, follow each Weiner link of the parent and test whether it has a child labeled c
            // if that is the case, that child's suffix link is v, and it is the Weiner link of v for its root label
            for(size_t i = 0; i < parent_data.weiner_links.size(); i++) {
                auto u = parent_data.weiner_links[i];
                /*
                    FIXME: if there was a way to also get the label of the Weiner link, we would have the root label right away
                    this could be done using a select query on inlined trie edge arrays, which is not (yet?) implemented
                    however, the root label is still required in STEP 1 when a suffix link has been found
                */
                FilterIndex link;
                if(topk.filter_node(u).children.try_get(c, link)) {
                    auto& link_data = topk.filter_node(link);
                    assert(link_data.suffix_link == DOES_NOT_EXIST); // if it already had a suffx link, it would have to be v, but v was only just created

                    link_data.suffix_link = v;
                    vdata.weiner_links.insert(link_data.root_label, link);
                }
            }
        }
    };

    topk.on_delete_node = [&](FilterIndex const v){
        assert(v); // cannot be the root
        auto& vdata = topk.filter_node(v);

        // STEP 1: delete Weiner link pointing to v
        if(vdata.suffix_link != DOES_NOT_EXIST) {
            auto& link_data = topk.filter_node(vdata.suffix_link);

            #ifndef NDEBUG
            FilterIndex x;
            assert(link_data.weiner_links.try_get(vdata.root_label, x));
            assert(x == v);
            #endif

            link_data.weiner_links.remove(vdata.root_label);
        }

        // STEP 2: delete suffix links pointing to v
        for(size_t i = 0; i < vdata.weiner_links.size(); i++) {
            auto& link_data = topk.filter_node(vdata.weiner_links[i]);
            assert(link_data.suffix_link == v);
            link_data.suffix_link = DOES_NOT_EXIST;
        }
    };

    // initialize encoding
    PhraseBlockWriter writer(out, block_size, true);

    // 
    tlx::RingBuffer<char> buffer(window_size);
    FilterIndex x = 0;
    size_t dx = 0;

    size_t xbegin = 0; // the position at which the current factor starts
    size_t xsrc = 0;   // the source position of the current reference

    auto write_literal = [&](char const c){
        if constexpr(DEBUG) std::cout << xbegin << ": LITERAL " << display(c) << std::endl;

        writer.write_len(0);
        writer.write_literal(c);

        ++num_phrases;
        ++num_literal;
        ++xbegin;
    };

    auto write_ref = [&](size_t const src, size_t const len){
        if constexpr(DEBUG) std::cout << xbegin << ": REFERENCE (" << src << "/" << xbegin - src << ", " << len << ")" << std::endl;

        writer.write_len(len);
        writer.write_ref(src);

        ++num_phrases;
        ++num_ref;
        xbegin += len;

        longest = std::max(longest, len);
        total_ref_len += len;
        total_ref_dist += src;
    };

    auto write_ref_or_literals = [&](size_t const src, size_t const len, FilterIndex const v){
        if(len >= threshold) {
            write_ref(src, len);
        } else {
            char buf[len] = "\0";
            topk.get(v, buf);
            for(size_t i = 0; i < len; i++) {
                write_literal(buf[len - 1 - i]);
            }
        }
    };

    auto handle = [&](char const c) {
        // append to ring buffer
        {
            if(buffer.size() == buffer.max_size()) {
                buffer.pop_front();
            }
            buffer.push_back(c);
        }

        if constexpr(PROTOCOL) std::cout << "handle character " << display(c) << " at i=" << n << ", the factor node is x=" << x << " at depth d(x)=" << dx << std::endl;

        auto const& xdata = topk.filter_node(x);
        FilterIndex v;
        Topk::StringState s;
        if(xdata.weiner_links.try_get(c, v)) {
            // Weiner link exists, extend LZ factor
            if constexpr(PROTOCOL) std::cout << "\tfollowed Weiner link with label " << display(c) << " to node v=" << v
                                             << " -> LZ phrase " << (num_phrases+1) << " continues" << std::endl;
            
            x = v;
            ++dx;
            s = topk.at(x, dx, c);

            auto const prev_occ = topk.filter_node(v).prev_occ;
            if(prev_occ < xbegin) xsrc = prev_occ;
        } else {
            if(dx >= 1) {
                write_ref_or_literals(xbegin - xsrc, dx, x);
                if constexpr(PROTOCOL) std::cout << "\tWeiner link with label " << display(c) << " not found -> LZ phrase " << num_phrases << " ends" << std::endl;
            }

            auto const& root = topk.filter_node(0);
            if(root.children.try_get(c, v)) {
                // known literal
                if constexpr(PROTOCOL) std::cout << "\tbeginning new phrase with known literal " << display(c) << std::endl;

                x = v;
                dx = 1;
                s = topk.at(x, dx, c);

                xsrc = topk.filter_node(v).prev_occ;
            } else {
                // new literal (-> literal phrase)
                write_literal(c);
                if constexpr(PROTOCOL) std::cout << "\tLZ phrase " << num_phrases << " is new literal " << display(c) << std::endl;

                x = 0;
                dx = 0;
                s = topk.empty_string();

                xsrc = 0;
            }
        }

        // insert remaining nodes for buffer until we drop out
        {
            auto d = dx;
            assert(n >= d);

            size_t occ;
            if(s.node) {
                if constexpr(!fast) {
                    // ascend in trie to update previous occurrences
                    occ = n - d + 1;
                    auto v = s.node;
                    while(v) {
                        auto* vdata = &topk.filter_node(v);
                        vdata->prev_occ = occ++;
                        v = vdata->parent;
                    }
                }
                occ = n - d;
            } else {
                occ = n;
            }

            while(d < buffer.size() && (s.frequent || s.new_node)) {
                auto const v = s.node;
                s = topk.extend(s, buffer[buffer.size() - d - 1]);
                if constexpr(PROTOCOL) std::cout << "\tinsert/follow edge labeled " << display(buffer[buffer.size() - d - 1]) << " from v=" << v << " to v'=" << s.node << std::endl;

                if(s.frequent || s.new_node) {
                    topk.filter_node(s.node).prev_occ = occ--;
                }

                if constexpr(fast) {
                    if(!s.new_node) break; // unless we a adding a new frequent string, don't bother
                }

                ++d;
            }

            topk.drop_out(s); // force count
        }
    };

    while(begin != end) {
        // read next character
        handle(*begin++);
        ++n;
    }

    // potentially encode final phrase
    if(dx >= 1) {
        write_ref_or_literals(xbegin - xsrc, dx, x);
    }

    writer.flush();
    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << n
        << ": num_ref=" << num_ref
        << ", num_literal=" << num_literal
        << " -> total phrases: " << num_phrases
        << ", longest_ref=" << longest
        << ", avg_ref_len=" << ((double)total_ref_len / (double)num_ref)
        << ", avg_ref_dist=" << ((double)total_ref_dist / (double)num_ref)
        << std::endl;
}

template<iopp::BitSource In, std::output_iterator<char> Out>
void lz77like_decompress(In in, Out out) {
    uint64_t const magic = in.read(64);
    if(magic != MAGIC) {
        std::cerr << "wrong magic: 0x" << std::hex << magic << " (expected: 0x" << MAGIC << ")" << std::endl;
        std::abort();
    }

    std::string dec; // yes, we do it in RAM...

    size_t num_ref = 0;
    size_t num_literal = 0;

    PhraseBlockReader reader(in, true);
    while(in) {
        auto len = reader.read_len();
        if(len > 0) {
            ++num_ref;

            auto const src = reader.read_ref();
            assert(src > 0);

            if constexpr(DEBUG) std::cout << dec.length() << ": REFERENCE (" << src << ", " << len << ")" << std::endl;            

            auto const i = dec.length();
            assert(i >= src);

            auto j = i - src;
            while(len--) dec.push_back(dec[j++]);
        } else {
            ++num_literal;

            auto const c = reader.read_literal();
            if constexpr(DEBUG) std::cout << dec.length() << ": LITERAL " << display(c) << std::endl;

            dec.push_back(c);
        }
    }

    // output
    std::copy(dec.begin(), dec.end(), out);

    // debug
    std::cout << "decompress"
        << ": n=" << dec.length()
        << ", num_ref=" << num_ref
        << ", num_literal=" << num_literal
        << " -> total phrases: " << (num_ref + num_literal)
        << std::endl;
}
