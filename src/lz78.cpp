#include <iostream>
#include <vector>

#include <iopp/file_input_stream.hpp>

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

int main(int argc, char** argv) {
    if(argc < 2) {
        std::cerr << "usage: " << argv[0] << " [FILE]" << std::endl;
        return -1;
    }

    TrieFCNS trie;
    TrieFCNS::Node u = trie.root();
    TrieFCNS::Node v;
    size_t num_phrases = 0;

    iopp::FileInputStream fis(argv[1]);
    for(char const c : fis) {
        if(trie.follow_edge(u, c, v)) {
            // edge exists, extend phrase
            u = v;
        } else {
            // edge doesn't exist, new LZ78 phrase
            ++num_phrases;
            u = trie.root();
        }
    }
    ++num_phrases; // final phrase

    std::cout << "num_phrases=" << num_phrases << std::endl;
}
