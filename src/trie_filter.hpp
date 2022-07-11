#include <concepts>
#include <cstddef>
#include <memory>

template<std::unsigned_integral Frequency>
class TrieFilter {
private:
    struct Node {
        char label;
        size_t first_child;
        size_t next_sibling;
        Frequency freq;
        Frequency insert_freq;
    };

    size_t capacity_;
    size_t size_;

    std::unique_ptr<Node[]> nodes_;

public:
    TrieFilter(size_t const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(size_t i = 0; i < capacity_; i++) {
            nodes_[i] = Node { (char)0, 0, 0, 0, 0 };
        }
    }

    void insert_child(size_t const node, size_t const parent, char const label, Frequency freq) {
        auto sibling = nodes_[parent].first_child;
        nodes_[parent].first_child = node;
        nodes_[node] = Node { label, 0, sibling, freq, freq };
    }

    size_t new_node() {
        return size_++;
    }

    bool try_get_child(size_t const node, char const label, size_t& out_child) {
        auto fc = nodes_[node].first_child;
        auto v = fc;
        while(v) {
            if(nodes_[v].label == label) {
                out_child = v;
                return true;
            }
            v = nodes_[v].next_sibling;
        }
        return false;
    }

    Frequency increment(size_t const node) {
        return ++nodes_[node].freq;
    }

    size_t root() const {
        return 0;
    }

    bool full() const { 
        return size_ == capacity_;
    }
};
