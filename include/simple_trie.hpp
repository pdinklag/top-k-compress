#pragma once

#include <vector>
#include "trie_edge_array.hpp"

class SimpleTrie {
public:
    using Node = size_t;
    using NodeData = TrieEdgeArray<char, Node>;

private:
    std::vector<NodeData> nodes_;

    Node insert_child(Node const parent, char const c) {
        auto const v = size();
        nodes_.push_back(NodeData());
        nodes_[parent].insert(c, v);
        return v;
    }

public:
    SimpleTrie() {
        clear();
    }

    Node root() const { return 0; }

    size_t size() const { return nodes_.size(); }

    bool follow_edge(Node const v, char const c, Node& out_node) {
        auto const found = nodes_[v].try_get(c, out_node);
        if(!found) {
            out_node = insert_child(v, c);
        }
        return found;
    }

    void clear() {
        nodes_.clear();
        nodes_.push_back(NodeData());
    }
};
