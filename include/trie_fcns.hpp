#pragma once

#include <vector>

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
        clear();
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

    void clear() {
        fc_.clear();
        ns_.clear();
        in_.clear();

        // create root
        fc_.push_back(0);
        ns_.push_back(0);
        in_.push_back(0);
    }
};
