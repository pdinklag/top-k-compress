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
    static constexpr bool DEBUG = true;

public:
    using StringView = FPStringView<Char>;

    // computes a hash for a string with the given length and fingerprint
    static constexpr uint64_t nav_hash(Index const len, uint64_t const fp) {
        return 963ULL * len - fp;
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
    std::vector<NodeNumber> phrase_leaves_;

    std::string extract_buffer_;

    NodeNumber create_node() {
        auto const i = nodes_.size();
        nodes_.emplace_back();
        return NodeNumber(i);
    }

    void insert_nav(NodeNumber const v, NodeNumber const parent, StringView const& s, Index const pos) {
        auto const p_v = rst(nodes_[v].len, max_i_rst(nodes_[v].len, nodes_[parent].len));
        auto const h_v = s.fingerprint(pos, pos + p_v - 1);

        if constexpr(DEBUG) {
            std::cout << "\t\tnav[" << p_v << ", 0x" << std::hex << h_v << std::dec << "] := " << v << std::endl;
        }

        auto const hash = nav_hash(p_v, h_v);

        assert(!nav_.contains(hash));
        nav_.emplace(hash, v);
        // nav_[hash] = v;
    }

    NodeNumber approx_find(StringView const& s, Index const pos, Index const len) const {
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
                    p += j;
                    v = it->second;
                }
            }
            j /= 2;
        }

        // potentially follow next edge
        {
            auto it = nodes_[v].map.find(s[pos + nodes_[v].len]);
            if(it != nodes_[v].map.end()) {
                v = it->second;
            }
        }

        return v;
    }

    NodeNumber nca(NodeNumber u, NodeNumber v) const {
        while(u != v) {
            if(nodes_[u].len > nodes_[v].len) {
                u = nodes_[u].parent;
            } else {
                v = nodes_[v].parent;
            }
        }
        return u;
    }

public:
    LZEndRevPhraseTrie(LZEndParsing<Char, Index> const& lzend) : lzend_(&lzend) {
        // ensure root
        create_node();

        // empty phrase 0 to offset phrase numbers
        phrase_leaves_.push_back(root_);
    }

    Index approx_find_phr(StringView const& s, Index const pos, Index const len) const {
        return nodes_[approx_find(s, pos, len)].phr;
    }

    Index nca_len(Index const p, Index const q) const {
        assert(p < phrase_leaves_.size());
        assert(q < phrase_leaves_.size());

        // translate phrase numbers to corresponding leaves
        auto const u = phrase_leaves_[p];
        auto const v = phrase_leaves_[q];

        assert(p == 0 || u != root_);
        assert(q == 0 || v != root_);

        return nodes_[nca(u, v)].len;
    }

    void insert(StringView const& s, Index const pos, Index const len) {
        auto const phr = (Index)phrase_leaves_.size();
        auto create_new_phrase_node = [&](){
            auto const x = create_node();
            nodes_[x].len = len;
            nodes_[x].phr = phr;
            phrase_leaves_.push_back(x);
            assert(phrase_leaves_[phr] == x);
            return x;
        };

        if constexpr(DEBUG) {
            std::cout << "\tTRIE: insert string " << phr << " of length " << len << " for phrase " << phr << ": " << s.string_view().substr(pos, len) << std::endl;
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
            insert_nav(x, 0, s, pos);

            nodes_[x].parent = root_;
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
            auto const extract_len = std::min(len, nodes_[v].len);
            {
                // extract suffix of phrase v
                // FIXME: compare more efficiently -- it can be done WHILE extracting the suffix and only until a mismatch
                extract_buffer_.clear();
                extract_buffer_.reserve(len);
                lzend_->extract_phrase_suffix(std::back_inserter(extract_buffer_), nodes_[v].phr, extract_len); // TODO: extract more efficiently and only until we mismatch?
                std::reverse(extract_buffer_.begin(), extract_buffer_.end());

                // compare
                common_suffix_length = 0;

                while(common_suffix_length < extract_len && extract_buffer_[common_suffix_length] == s[pos + common_suffix_length]) {
                    ++common_suffix_length;
                }
                assert(common_suffix_length >= 1); // we must have matched the first character in the root, otherwise we wouldn't be here

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
                {
                    auto const c = UChar(s[pos + nodes_[parent].len]);
                    assert(nodes_[parent].map.contains(c));
                    assert(nodes_[parent].map[c] == v);
                    nodes_[parent].map[c] = u;

                    nodes_[u].parent = parent;

                    insert_nav(u, parent, s, pos);
                }

                // make v a child of new node u
                {
                    auto const c = UChar(extract_buffer_[common_suffix_length]);
                    nodes_[u].map.emplace(c, v);
                    nodes_[v].parent = u;
                }
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
                insert_nav(x, u, s, pos);
            } else {
                assert(len == nodes_[u].len);

                // the string is already contained in the trie!
                if constexpr(DEBUG) {
                    std::cout << "\t\tstring already in trie at node " << u << " representing phrase " << nodes_[u].phr << std::endl;
                }
                phrase_leaves_.push_back(u);
                return;
            }
        }
    }
};
