#pragma once

#include <bit>
#include <cassert>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include <ankerl/unordered_dense.h>

#include <tdc/util/concepts.hpp>

#include <fp_string_view.hpp>
#include <lzend_parsing.hpp>

// reset the i least significant bits of x
template<std::unsigned_integral Index = uint32_t>
Index rst(Index const x, Index const i) {
    return x & ~((Index(1) << i) - 1);
}

// given x > y, find the maximum i such that rst(x, i) > y
template<std::unsigned_integral Index = uint32_t>
Index max_i_rst(Index const x, Index const y) {
    assert(x > y);
    
    static constexpr auto w = std::numeric_limits<Index>::digits;
    return w - std::countl_zero(x ^ y) - 1; // find the most significant bit that differs; any less significant bit may be reset
}

/// Implements the compact trie described in [Kempa & Koslobov, 2017]
template<std::integral Char = char, std::unsigned_integral Index = uint32_t>
class LZEndRevPhraseTrie {
private:
    static constexpr bool DEBUG = false;
    static constexpr bool PARANOID = true;

public:
    using StringView = FPStringView<Char>;

    // computes a hash for a string with the given length and fingerprint
    static constexpr uint64_t nav_hash(Index const len, uint64_t const fp) {
        return uint64_t(len) * 68719476377ULL + fp * 262127ULL;
    }

    using NodeNumber = Index;

private:
    using UChar = std::make_unsigned_t<Char>;

    static constexpr NodeNumber root_ = 0;

    struct Node {
        Index len;
        Index phr;
        NodeNumber parent;
        ankerl::unordered_dense::map<UChar, NodeNumber> map;

        Node() : len(0), phr(0), parent(root_) {
        }

        bool try_get(UChar const c, NodeNumber& out) const {
            auto it = map.find(c);
            if(it != map.end()) {
                out = it->second;
                return true;
            } else {
                return false;
            }
        }
    };

    LZEndParsing<Char, Index> const* lzend_;

    ankerl::unordered_dense::map<uint64_t, NodeNumber> nav_;
    std::vector<Node> nodes_;
    std::vector<NodeNumber> phrase_nodes_;

    NodeNumber create_node() {
        auto const i = nodes_.size();
        nodes_.emplace_back();
        return NodeNumber(i);
    }

    Index compute_pv(NodeNumber const v, NodeNumber const parent) {
        return rst(nodes_[v].len, max_i_rst(nodes_[v].len, nodes_[parent].len));
    }

    void update_nav(NodeNumber const v, Index const p_v, Index const h_v) {
        if constexpr(DEBUG) {
            std::cout << "\t\tnav[" << p_v << ", 0x" << std::hex << h_v << std::dec << "] := " << v << std::endl;
        }

        auto const hash = nav_hash(p_v, h_v);
        nav_[hash] = v;
    }

    std::pair<Index, uint64_t> update_nav(NodeNumber const v, NodeNumber const parent, StringView const& s, Index const pos) {
        auto const p_v = std::min(compute_pv(v, parent), Index(s.length() - pos));
        assert(p_v > nodes_[parent].len);

        auto const h_v = s.fingerprint(pos, pos + p_v - 1);
        update_nav(v, p_v, h_v);
        return { p_v, h_v };
    }

    NodeNumber approx_find(StringView const& s, Index const pos, Index const len, Index& hash_match) const {
        if constexpr(DEBUG) {
            std::cout << "\tTRIE: approx_find for string of length " << len << ": " << s.string_view().substr(pos, len) << std::endl;
        }

        hash_match = 0;
        Index p = 0;
        auto v = root_;

        // fat binary search for deepest node v such that str(v) is a prefix of s
        auto j = std::bit_floor(len); // j will be a power of two
        while(j) {
            if(nodes_[v].len >= p + j) {
                p += j;
            } else if(p + j < len) {
                assert(pos + p + j - 1 < s.length());
                auto const h = s.fingerprint(pos, pos + p + j - 1);
                auto it = nav_.find(nav_hash(p + j, h));
                if(it != nav_.end()) {
                    if constexpr(DEBUG) {
                        std::cout << "\t\tfollowed nav[" << (p+j) << ", 0x" << std::hex << h << std::dec << "] to node " << it->second << " representing phrase " << nodes_[it->second].phr << std::endl;
                    }

                    p += j;
                    v = it->second;

                    hash_match = p;
                }
            }
            j /= 2;
        }

        // potentially follow next edge
        {
            auto it = nodes_[v].map.find(s[pos + nodes_[v].len]);
            if(it != nodes_[v].map.end()) {
                if constexpr(DEBUG) {
                    std::cout << "\t\tfollowed outgoing edge of node " << v << " for initial character " << display(s[pos + nodes_[v].len])
                        << " at depth " << (pos + nodes_[v].len) << " to node " << it->second << " representing phrase " << nodes_[it->second].phr << std::endl;
                }

                v = it->second;
            }
        }

        if constexpr(DEBUG) {
            if(v == root_) std::cout << "\t\tfound nothing" << std::endl;
        }

        return v;
    }

    NodeNumber nca(NodeNumber const in_u, NodeNumber const in_v) const {
        auto u = in_u;
        auto v = in_v;

        while(u != v) {
            if(nodes_[u].len > nodes_[v].len) {
                u = nodes_[u].parent;
            } else {
                v = nodes_[v].parent;
            }
        }
        if constexpr(DEBUG) std::cout << "\t\tnca of node " << in_u << " and node " << in_v << " is node " << u << " at depth " << nodes_[u].len << std::endl;
        return u;
    }

public:
    LZEndRevPhraseTrie(LZEndParsing<Char, Index> const& lzend) : lzend_(&lzend) {
        // ensure root
        create_node();

        // empty phrase 0 to offset phrase numbers
        phrase_nodes_.push_back(root_);
    }

    Index approx_find_phr(StringView const& s, Index const pos, Index const len, Index& hash_match) const {
        return nodes_[approx_find(s, pos, len, hash_match)].phr;
    }

    Index approx_find_phr(StringView const& s, Index const pos, Index const len) const {
        Index discard;
        return approx_find_phr(s, pos, len, discard);
    }

    Index nca_len(Index const p, Index const q) const {
        assert(p < phrase_nodes_.size());
        assert(q < phrase_nodes_.size());

        // translate phrase numbers to corresponding leaves
        auto const u = phrase_nodes_[p];
        auto const v = phrase_nodes_[q];

        assert(p == 0 || u != root_);
        assert(q == 0 || v != root_);

        return nodes_[nca(u, v)].len;
    }

#ifndef NDEBUG
    void verify_edge_integrity(NodeNumber const v) const {
        // verify that we can reach v from its parent using the first character on the corresponding edge
        auto const parent = nodes_[v].parent;

        // get the first character on the edge from parent to v by decoding it from the reversed phrase suffix
        char alpha;
        lzend_->extract_reverse_phrase_suffix_until([&](char const c){
            alpha = c;
            return true; // nb: continue decoding till the end
        }, nodes_[v].phr, nodes_[parent].len + 1);

        // try find the edge for the character and retrieve the connected node
        NodeNumber reached;
        bool const edge_exists = nodes_[parent].try_get(UChar(alpha), reached);

        assert(edge_exists);
        assert(reached == v);
    }
#endif

    void insert(StringView const& s, Index const pos, Index const len) {
        auto const phr = (Index)phrase_nodes_.size();
        auto create_new_phrase_node = [&](){
            auto const x = create_node();
            nodes_[x].len = len;
            nodes_[x].phr = phr;
            phrase_nodes_.push_back(x);
            return x;
        };

        if constexpr(DEBUG) {
            std::cout << "\tTRIE: insert string of length " << len << " for phrase " << phr << ": " << s.string_view().substr(pos, len) << std::endl;
        }

        auto v = root_;
        NodeNumber parent = -1;
        size_t d = 0;

        NodeNumber found;
        while(d < len && nodes_[v].try_get(UChar(s[pos + d]), found)) {
            parent = v;
            v = found;
            d = nodes_[v].len;
        }

        if constexpr(DEBUG) {
            std::cout << "\t\tblindly descended to node " << v << " at depth " << d << std::endl;
        }

        if(v == root_) {
            // v is the root, which means that no prefix of s is contained in the trie
            assert(d == 0);

            auto const x = create_new_phrase_node();
            if constexpr(DEBUG) {
                std::cout << "\t\tcreating new node " << x << " at depth " << len << " representing the new phrase as child of root" << std::endl;
            }

            nodes_[root_].map.emplace(UChar(s[pos]), x);
            update_nav(x, 0, s, pos);

            nodes_[x].parent = root_;

            #ifndef NDEBUG
            if constexpr(PARANOID) verify_edge_integrity(x);
            #endif
        } else {
            // v is the deepest possible node such that str(v) shares a common prefix with s that <= |str(v)|
            assert(s[pos] == (*lzend_)[nodes_[v].phr].last); // if these don't match, something is seriously off

            // we need to find the exact length of that common prefix
            // according to [Kempa & Kosolobov, 2017], we extract the relevant portion of an underlying phrase's suffix (via LZEnd's decoding mechanism),
            // and compare it to the phrase to be inserted
            // 
            // CAUTION:
            // - in the top-k scenario, we no longer store ALL phrases, but only frequent phrases
            // - in order to be able to decode a phrase, if a phrase is frequent, all of its dependencies must be frequent
            // - this requires a new removal mechanism:
            //   - not only does a node to be removed have to be a leaf
            //   - it must furthermore represent a phrase that is not used by any other phrase in the trie
            // - SOLUTION: only keep unused phrases represented by a leaf in the PQ
            Index common_suffix_length;
            char mismatch;

            auto const extract_len = std::min(len + 1, nodes_[v].len); // nb: +1, because if we find no mismatch, we want the next character to be a mismatch
            {
                // extract reverse suffix of phrase v while we're matching with the reverse input string
                common_suffix_length = 0;
                lzend_->extract_reverse_phrase_suffix_until([&](char const c){
                    mismatch = c;

                    if(common_suffix_length < len && c == s[pos + common_suffix_length]) {
                        ++common_suffix_length;
                        return true;
                    } else {
                        return false;
                    }
                }, nodes_[v].phr, extract_len);

                assert(common_suffix_length >= 1); // we must have matched the first character in the root, otherwise we wouldn't be here
                assert(common_suffix_length <= len); // we cannot have matched more characters than the inserted string has

                if constexpr(DEBUG) {
                    std::cout << "\t\tcomputed common_suffix_length=" << common_suffix_length << std::endl;
                }

                // navigate back up in the trie until the depth matches
                while(v != root_ && nodes_[parent].len >= common_suffix_length) {
                    v = parent;
                    parent = nodes_[parent].parent;
                }
                assert(v != root_);

                if constexpr(DEBUG) {
                    std::cout << "\t\tascended to node " << v << " at depth " << nodes_[v].len << std::endl;
                }
            }
            assert(common_suffix_length > nodes_[parent].len);
            assert(common_suffix_length <= nodes_[v].len);

            // determine the node to which to add a child
            NodeNumber u;
            if(common_suffix_length < nodes_[v].len) {
                // we split the edge from parent to v and create a new inner node

                u = create_node();
                nodes_[u].len = common_suffix_length;
                nodes_[u].phr = nodes_[v].phr; // propagate any child's phrase number - we might as well use the new phr

                if constexpr(DEBUG) {
                    std::cout << "\t\tcreating new inner node " << u << " representing phrase " << nodes_[u].phr << " at depth " << common_suffix_length << " on edge from node " << parent << " to node " << v << std::endl;
                }

                // replace v by u as child of parent
                Index p_u;
                uint64_t h_u;
                {
                    auto const c = UChar(s[pos + nodes_[parent].len]);
                    assert(nodes_[parent].map.contains(c));
                    assert(nodes_[parent].map[c] == v);
                    nodes_[parent].map[c] = u;

                    nodes_[u].parent = parent;

                    // update anv
                    // it is OK to use s for fingerprint computation, because u marks the common suffix
                    std::tie(p_u, h_u) = update_nav(u, parent, s, pos);
                }

                // make v a child of new node u
                {
                    auto const c = UChar(mismatch);
                    nodes_[u].map.emplace(c, v);
                    nodes_[v].parent = u;

                    auto const p_v = compute_pv(v, u);
                    if(p_v <= common_suffix_length) {
                        // we must update nav for v
                        // HOWEVER, we MUST NOT use s for fingerprint computation beyond the common suffix length,
                        // because it does NOT match the phrase represented by v anymore

                        // instead, we compute h_v by reconstructing the correct string from the encoding
                        // TODO: use h_u as a seed - the first characters do match, there is no need to reconstruct them
                        uint64_t h_v = 0;
                        lzend_->extract_reverse_phrase_suffix_until([&](char const c){
                            h_v = StringView::append(h_v, c);
                            return true;
                        }, v, p_v);

                        update_nav(v, p_v, h_v);
                    }
                }

                #ifndef NDEBUG
                if constexpr(PARANOID) verify_edge_integrity(u);
                if constexpr(PARANOID) verify_edge_integrity(v);
                #endif
            } else {
                // we add a new child directly to v
                u = v;
            }

            if(len > nodes_[u].len) {
                auto const x = create_new_phrase_node();
                if constexpr(DEBUG) {
                    std::cout << "\t\tcreating new node " << x << " at depth " << len << " representing phrase " << phr << " as child of node " << u << " at depth " << nodes_[u].len << std::endl;
                }

                // make x a child of u
                auto const c = UChar(s[pos + common_suffix_length]);
                assert(!nodes_[u].map.contains(c));
                nodes_[u].map.emplace(c, x);

                nodes_[x].parent = u;
                update_nav(x, u, s, pos);

                #ifndef NDEBUG
                if constexpr(PARANOID) verify_edge_integrity(x);
                #endif
            } else {
                assert(len == nodes_[u].len);
                // the string is already contained in the trie
                // particularly, this happens if the inner node splitting an edge is exactly as deep as the inserted string
                phrase_nodes_.push_back(u);
            }
        }
        assert(phrase_nodes_.size() == phr + 1); // we must have created a new entry here
    }
};
