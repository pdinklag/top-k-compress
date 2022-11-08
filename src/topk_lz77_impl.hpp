#include "topk_common.hpp"

#include <tlx/container/ring_buffer.hpp>

constexpr uint64_t MAGIC =
    ((uint64_t)'T') << 56 |
    ((uint64_t)'O') << 48 |
    ((uint64_t)'P') << 40 |
    ((uint64_t)'K') << 32 |
    ((uint64_t)'L') << 24 |
    ((uint64_t)'Z') << 16 |
    ((uint64_t)'7') << 8 |
    ((uint64_t)'7');

constexpr bool PROTOCOL = true;

struct TopkLZ77TrieNode : public TopkTrieNode<> {
    using TopkTrieNode::Character;
    using TopkTrieNode::Index;

    using TopkTrieNode::TopkTrieNode;

    using WeinerLinkArray = TrieEdgeArray<Character, Index, TrieNode::Size>;

    Index           suffix_link;
    WeinerLinkArray weiner_links;
    Character       root_label;

} __attribute__((packed));

template<tdc::InputIterator<char> In, iopp::BitSink Out>
void topk_compress_lz77(In begin, In const& end, Out out, size_t const k, size_t const window_size, size_t const num_sketches, size_t const sketch_rows, size_t const sketch_columns, size_t const block_size) {
    using namespace tdc::code;

    pm::MallocCounter malloc_counter;
    malloc_counter.start();

    TopkFormat f(k, 0, num_sketches, sketch_rows, sketch_columns, false);
    f.encode_header(out, MAGIC);

    // initialize compression
    // - frequent substring 0 is reserved to indicate a literal character
    using Topk = TopKSubstrings<TopkLZ77TrieNode>;
    using FilterIndex = TopkLZ77TrieNode::Index;

    constexpr FilterIndex DOES_NOT_EXIST = -1;

    Topk topk(k, num_sketches, sketch_rows, sketch_columns);
    size_t n = 0;
    size_t num_phrases = 0;
    size_t longest = 0;

    // the root has no suffix link
    topk.filter_node(0).suffix_link = DOES_NOT_EXIST;

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

    topk.on_filter_node_deleted = [&](FilterIndex const v){
        assert(v); // cannot be the root
        auto& vdata = topk.filter_node(v);

        // TODO: delete Weiner link pointing here
        // TODO: delete suffix links pointing here
    };

    // initialize encoding
    PhraseBlockWriter writer(out, block_size);

    // 
    tlx::RingBuffer<char> buffer(window_size);
    Topk::StringState s = topk.empty_string();
    Topk::StringState x = topk.empty_string();    

    auto handle = [&](char const c) {
        // append to ring buffer
        {
            if(buffer.size() == buffer.max_size()) {
                buffer.pop_front();
            }
            buffer.push_back(c);
        }

        // traverse up the trie and find node with Weiner link labeled c
        FilterIndex link;

        auto dv = s.len;
        auto v = s.node;
        if constexpr(PROTOCOL) std::cout << "handle character " << display(c) << " at i=" << n << ", the current node is v=" << v << " at depth d(v)=" << dv
                                         << ", the factor node is x=" << x.node << " at depth d(x)=" << x.len << std::endl;

        auto const* vdata = &topk.filter_node(v);
        while(v && !vdata->weiner_links.try_get(c, link)) {
            v = vdata->parent;
            vdata = &topk.filter_node(v);

            assert(dv);
            --dv;
        }
        if constexpr(PROTOCOL) std::cout << "\tascended to node v=" << v << " at depth d(v)=" << dv << std::endl;

        bool has_link = vdata->weiner_links.try_get(c, link);
        if(has_link) {
            // follow link
            v = link;
            vdata = &topk.filter_node(v);
            ++dv; // following the Weiner link increases the depth by 1

            if constexpr(PROTOCOL) std::cout << "\tfollowed Weiner link with label " << display(c) << " to node v=" << v << " at depth d(v)=" << dv << std::endl;

            // descend down as far as possible using characters of S in reverse order
            assert(buffer.size() >= dv);
            size_t i = buffer.size() - dv;

            FilterIndex child;
            while(i && vdata->children.try_get(buffer[i-1], child)) {
                ++dv;
                v = child;
                vdata = &topk.filter_node(v);
                --i;
            }
            if constexpr(PROTOCOL) std::cout << "\tdescended to node " << v << " at depth d(v)=" << dv << std::endl;
        } else {
            // v must be the root and there is no edge with label c
            if constexpr(PROTOCOL) std::cout << "\tno Weiner link found, concluding that v is the root" << std::endl;
            assert(v == 0);
        }

        if(x.node) {
            if(dv > x.len) {
                if constexpr(PROTOCOL) std::cout << "\td(v)=" << dv << " > d(x)=" << x.len << " -> LZ phrase " << (num_phrases+1) << " continues" << std::endl;
            } else {
                if constexpr(PROTOCOL) std::cout << "\td(v)=" << dv << " <= d(x)=" << x.len << " -> LZ phrase " << (num_phrases+1) << " ends" << std::endl;
                ++num_phrases;
            }
        }

        s = topk.at(v);
        x = s;

        {
            // insert remaining nodes for buffer until we drop out
            auto ss = s;
            while(dv < buffer.size() && (ss.frequent || ss.new_node)) {
                if constexpr(PROTOCOL) std::cout << "\tinsert new edge labeled " << display(buffer[buffer.size() - dv - 1]) << std::endl;
                ss = topk.extend(ss, buffer[buffer.size() - dv - 1]);
                ++dv;
            }
        }

        if(!x.node) {
            if constexpr(PROTOCOL) std::cout << "\tLZ phrase " << (num_phrases+1) << " is literal " << display(c) << std::endl;
            ++num_phrases;
        }
    };

    while(begin != end) {
        // read next character
        ++n;
        handle(*begin++);
    }

    // TODO: potentially encode final phrase
    ++num_phrases;
    if constexpr(PROTOCOL) std::cout << "done factorizing, num_phrases=" << num_phrases << std::endl;

    writer.flush();
    malloc_counter.stop();

    topk.print_debug_info();
    std::cout << "mem_peak=" << malloc_counter.peak() << std::endl;
    std::cout << "parse"
        << " n=" << n
        << " -> num_phrases=" << num_phrases
        << ", longest=" << longest
        << std::endl;
}
