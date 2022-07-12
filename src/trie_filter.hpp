#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <memory>
#include <string>

template<std::unsigned_integral Frequency>
class TrieFilter {
private:
    struct Node {
        size_t first_child;
        size_t next_sibling;
        char label;
        Frequency freq;
        Frequency insert_freq;
        size_t parent;
        uint64_t fingerprint;
    };

    size_t capacity_;
    size_t size_;

    std::unique_ptr<Node[]> nodes_;

public:
    struct ExtractInfo {
        size_t parent;
        Frequency freq_delta;
        uint64_t fingerprint;
    };

    TrieFilter(size_t const capacity) : capacity_(capacity), size_(1) {
        nodes_ = std::make_unique<Node[]>(capacity_);
        for(size_t i = 0; i < capacity_; i++) {
            nodes_[i] = Node { 0, 0, (char)0, 0, 0, 0, 0 };
        }
    }

    void insert_child(size_t const node, size_t const parent, char const label, Frequency freq, uint64_t fingerprint) {
        size_t discard;
        assert(!try_get_child<false>(parent, label, discard));

        auto sibling = nodes_[parent].first_child;
        nodes_[parent].first_child = node;
        nodes_[node] = Node { 0, sibling, label, freq, freq, parent, fingerprint };

        assert(is_child_of(node, parent));
        assert(is_leaf(node));
    }

    ExtractInfo extract(size_t const node) {
        assert(is_leaf(node)); // cannot extract an inner node

        auto const parent = nodes_[node].parent;
        assert(is_child_of(node, parent));

        auto fc = nodes_[parent].first_child;
        if(fc == node) {
            // removing the first child
            nodes_[parent].first_child = nodes_[node].next_sibling;
        } else {
            // removing some other child
            // find previous sibling and remove
            auto prev_sibling = fc;
            auto v = nodes_[fc].next_sibling;
            while(v && v != node) {
                prev_sibling = v;
                v = nodes_[v].next_sibling;
            }

            if(v == node) {
                nodes_[prev_sibling].next_sibling = nodes_[node].next_sibling;
            } else {
                // "this kid is not my son"
                assert(false);
            }
        }

        assert(nodes_[node].freq >= nodes_[node].insert_freq);
        return ExtractInfo {
            parent,
            nodes_[node].freq - nodes_[node].insert_freq,
            nodes_[node].fingerprint
        };
    }

    size_t new_node() {
        return size_++;
    }

    template<bool mtf = true>
    bool try_get_child(size_t const node, char const label, size_t& out_child) {
        auto const fc = nodes_[node].first_child;
        auto v = fc;
        size_t ps = 0;
        while(v) {
            auto const ns = nodes_[v].next_sibling;
            if(nodes_[v].label == label) {
                out_child = v;

                if(mtf && v != fc) {
                    assert(ps);

                    // MTF
                    nodes_[node].first_child = v;
                    nodes_[v].next_sibling = fc;
                    nodes_[ps].next_sibling = ns;

                    assert(is_child_of(v, node));
                    assert(is_child_of(fc, node));
                    assert(is_child_of(ps, node));
                    if(ns) assert(is_child_of(ns, node));  
                }
                return true;
            }
            ps = v;
            v = ns;
        }
        return false;
    }

    Frequency increment(size_t const node) {
        return ++nodes_[node].freq;
    }

    bool is_leaf(size_t const node) const {
        return nodes_[node].first_child == 0;
    }

    bool is_child_of(size_t const node, size_t const parent) const {
        auto fc = nodes_[parent].first_child;
        if(fc == node) return true;

        auto v = nodes_[fc].next_sibling;
        while(v && v != node) {
            v = nodes_[v].next_sibling;
        }
        return v == node;
    }

    size_t root() const {
        return 0;
    }

    bool full() const { 
        return size_ == capacity_;
    }

    Frequency freq(size_t const node) const {
        return nodes_[node].freq;
    }

    size_t spell(size_t const node, char* buffer) const {
        // spell reverse
        size_t d = 0;
        auto v = node;
        while(v) {
            buffer[d++] = nodes_[v].label;
            v = nodes_[v].parent;
        }

        // reverse and return length
        std::reverse(buffer, buffer + d);
        return d;
    }
};
