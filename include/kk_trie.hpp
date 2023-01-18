#pragma once

#include <bit>
#include <cassert>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>
#include <vector>

#include <ankerl/unordered_dense.h>

#include <tdc/util/concepts.hpp>

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
class KKTrie {
public:
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
                out = it.second;
                return true;
            } else {
                return false;
            }
        }
    };

    struct NavKey {
        Index    len;
        uint64_t hash; 
    } __attribute__((__packed__));

    ankerl::unordered_dense::map<NavKey, NodeNumber> nav_;
    std::vector<Node> nodes_;
    std::vector<NodeNumber> phrase_leaves_;

    NodeNumber create_node() {
        auto const i = nodes_.size();
        nodes_.emplace_back();
        return NodeNumber(i);
    }

    void insert_nav(NodeNumber const v, NodeNumber const parent, std::string_view const& s) {
        auto const p_v = rst(nodes_[v].len, max_i_rst(nodes_[v].len, nodes_[parent].len));
        auto const h_v = 0; // TODO: compute fingerprint of s[0..p_v-1]
        auto const hash = nav_hash(p_v, h_v);
        assert(!nav_.contains(hash));

        nav_.emplace(hash, v);
    }

    NodeNumber approx_find(std::string_view const& s) const {
        auto const len = s.length();

        Index p = 0;
        auto v = root_;

        // fat binary search for deepest node v such that str(v) is a prefix of s
        auto j = std::bit_floor(s.length()); // j will be a power of two
        while(j) {
            if(nodes_[v].len >= p + j) {
                p += j;
            } else {
                auto const h = 0; // TODO: computer fingerprint of s[0..p+j-1]
                auto it = nav_.find(nav_hash(p + j, h));
                if(it != nav_.end()) {
                    p += j;
                    v = it.second;
                }
            }
            j /= 2;
        }

        // potentially follow next edge
        {
            auto it = nodes_[v].map.find(s[nodes_[v].len]);
            if(it != nodes_[v].map.end()) {
                v = it.second;
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
    KKTrie() {
        // ensure root
        create_node();
    }

    Index approx_find_phr(std::string_view const& s) const {
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

    Index insert_phrase(std::string_view const& s) {
        auto const phr = (Index)phrase_leaves_.size();
        auto const len = s.length();

        auto v = root_;
        NodeNumber parent = -1;
        size_t d = v.len;

        NodeNumber found;
        while(d < len && nodes_[v].try_get(UChar(s[d]), found)) {
            parent = v;
            v = found;
            d = nodes_[v].len;
        }

        auto const x = create_node();
        if(v == root_) {
            // v is the root, which means that no prefix of s is contained in the trie
            assert(d == 0);

            nodes_[root_].map.emplace(UChar(s[0]), x);
            insert_nav(x, 0, s);

            nodes_[x].parent = root_;
        } else {
            // v is the deepest possible node such that str(v) shares a common prefix with s that is at least |str(v)| (but shorter than |s|)
            assert(nodes_[v].len < len);

            Index common_prefix_length;
            {
                // we need to find the exact length of that common prefix
                common_prefix_length = 0; // TODO
            }
            assert(common_prefix_length >= nodes_[v].len);
            assert(common_prefix_length < len);

            // determine the node to which to add a child
            NodeNumber u;
            if(common_prefix_length > nodes_[v].len) {
                // we split the edge from parent to v and create a new inner node
                u = create_node();

                nodes_[u].len = common_prefix_length;
                nodes_[u].phr = nodes_[v].phr; // propagate any child's phrase number - we might as well use the new phr

                // replace v by u as child of parent
                {
                    auto const c = UChar(s[nodes_[parent].len]);
                    assert(nodes_[parent].map.contains(c));
                    assert(nodes_[parent].map[c] == v);
                    nodes_[parent].map[c] = u;

                    nodes_[u].parent = parent;
                    insert_nav(u, v, s);
                }

                // make v a child of new node u
                {
                    auto const c = UChar(s[common_prefix_length]);
                    nodes_[u].map.emplace(c, v);
                    nodes_[v].parent = u;
                    insert_nav(u, v, s);
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

        nodes_[x].len = len;
        nodes_[x].phr = phr;
        phrase_leaves_.push_back(x);
        assert(phrase_leaves_[phr] == x);
        
        return phr;
    }
};
