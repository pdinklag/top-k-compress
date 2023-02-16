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
public:
    using StringView = FPStringView<Char>;

    // computes a hash for a string with the given length and fingerprint
    static constexpr uint64_t nav_hash(Index const len, uint64_t const fp) {
        return len - fp; // TODO
    }

private:
    using NodeNumber = Index;
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

    void insert_nav(NodeNumber const v, NodeNumber const parent, StringView const& s) {
        auto const p_v = rst(nodes_[v].len, max_i_rst(nodes_[v].len, nodes_[parent].len));
        auto const h_v = s.fingerprint(p_v - 1);
        auto const hash = nav_hash(p_v, h_v);
        assert(!nav_.contains(hash));

        nav_.emplace(hash, v);
    }

    NodeNumber approx_find(StringView const& s) const {
        auto const len = s.length();

        Index p = 0;
        auto v = root_;

        // fat binary search for deepest node v such that str(v) is a prefix of s
        auto j = std::bit_floor(s.length()); // j will be a power of two
        while(j) {
            if(nodes_[v].len >= p + j) {
                p += j;
            } else {
                auto const h = s.fingerprint(p + j - 1);
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
            auto it = nodes_[v].map.find(s[nodes_[v].len]);
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

    Index approx_find_phr(StringView const& s) const {
        return nodes_[approx_find(s)].phr;
    }

    Index nca_len(Index const p, Index const q) const {
        // translate phrase numbers to corresponding leaves
        auto const u = phrase_leaves_[p];
        auto const v = phrase_leaves_[q];

        assert(u != root_);
        assert(v != root_);

        return nodes_[nca(u, v)].len;
    }

    Index insert(StringView const& s) {
        auto const phr = (Index)phrase_leaves_.size();
        auto const len = s.length();

        auto v = root_;
        NodeNumber parent = -1;
        size_t d = nodes_[v].len;

        NodeNumber found;
        while(d < len && nodes_[v].try_get(UChar(s[d]), found)) {
            parent = v;
            v = found;
            d = nodes_[v].len;
        }

        auto const x = create_node();
        nodes_[x].len = len;
        nodes_[x].phr = phr;

        if(v == root_) {
            // v is the root, which means that no prefix of s is contained in the trie
            assert(d == 0);

            nodes_[root_].map.emplace(UChar(s[0]), x);
            insert_nav(x, 0, s);

            nodes_[x].parent = root_;
        } else {
            // v is the deepest possible node such that str(v) shares a common prefix with s that is > |str(parent)| and <= |str(v)|
            assert(nodes_[v].len < len);

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
            Index common_prefix_length;
            auto const extract_len = nodes_[v].len - nodes_[parent].len;
            {
                // extract suffix of phrase v
                // TODO: assert that extract_len isn't "too long" -- the paper implies that it shouldn't exceed lg l, but WHY? (by the structure of rst / max_i_rst ?)
                extract_buffer_.clear();
                extract_buffer_.reserve(extract_len);
                lzend_->extract_phrase_suffix(std::back_inserter(extract_buffer_), nodes_[v].phr, extract_len); // TODO: extract more efficiently and only until we mismatch?
                std::reverse(extract_buffer_.begin(), extract_buffer_.end());

                // compare
                common_prefix_length = nodes_[parent].len;

                size_t i = 0; // position in extracted phrase (REVERSE)
                size_t j = 0; // offset position in phrase to be inserted (REVERSE)
                while(j < extract_len && extract_buffer_[i] == s[common_prefix_length + j]) {
                    ++j;
                    ++i;
                }
                common_prefix_length += j;
            }
            assert(common_prefix_length > nodes_[parent].len);
            assert(common_prefix_length <= nodes_[v].len);

            // determine the node to which to add a child
            NodeNumber u;
            if(common_prefix_length < nodes_[v].len) {
                // we split the edge from parent to v and create a new inner node
                u = create_node();

                nodes_[u].len = common_prefix_length;
                nodes_[u].phr = nodes_[v].phr; // propagate any child's phrase number - we might as well use the new phr

                // replace v by u as child of parent
                // nb: I first thought that the nav entry for v has to be removed and recomputed
                //     that would be difficult, because we don't have its entire string
                //     however, by construction of the trie via rst with max_i_rst, this should *never* be necessary!
                {
                    auto const c = UChar(s[nodes_[parent].len]);
                    assert(nodes_[parent].map.contains(c));
                    assert(nodes_[parent].map[c] == v);
                    nodes_[parent].map[c] = u;

                    nodes_[u].parent = parent;

                    insert_nav(u, parent, s);
                }

                // make v a child of new node u
                {
                    auto const c = UChar(extract_buffer_[common_prefix_length - nodes_[parent].len]);
                    nodes_[u].map.emplace(c, v);
                    nodes_[v].parent = u;
                }
            } else {
                // we add a new child directly to v
                u = v;
            }

            // make x a child of u
            auto const c = UChar(s[common_prefix_length]);
            assert(!nodes_[u].map.contains(c));
            nodes_[u].map.emplace(c, x);

            nodes_[x].parent = u;
            insert_nav(x, u, s);
        }

        phrase_leaves_.push_back(x);
        assert(phrase_leaves_[phr] == x);
        
        return phr;
    }
};
