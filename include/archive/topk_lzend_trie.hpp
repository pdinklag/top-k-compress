#pragma once

#include <memory>
#include <display.hpp>

#include <archive/topk_substrings.hpp>
#include <archive/topk_trie_node.hpp>

#include <archive/fp_string_view.hpp>

template<std::unsigned_integral Index = uint32_t>
class TopKLZEndTrie : public TopKSubstrings<TopkTrieNode<Index>> {
private:
    static constexpr bool DEBUG = false;

    using Base = TopKSubstrings<TopkTrieNode<Index>>;
    using FilterIndex = Base::FilterIndex;

    std::unique_ptr<Index[]> depth_;

    size_t insert_seq_ = 0;

#ifndef NDEBUG
    Index naive_depth(Index v) const {
        auto const& trie = Base::filter();
        Index d = 0;
        while(v) {
            ++d;
            v = trie.parent(v);
        }
        return d;
    }
#endif

public:
    using StringView = FPStringView<char>;

    inline TopKLZEndTrie(size_t const k, size_t const sketch_rows, size_t const sketch_columns, size_t const fp_window_size = 8)
        : Base(k, sketch_rows, sketch_columns, fp_window_size),
          depth_(std::make_unique<Index[]>(k)) {
        
        depth_[0] = 0;
 
        Base::on_filter_node_inserted = [&](Index const v){
            auto const parent = Base::filter().parent(v);
            depth_[v] = depth_[parent] + 1;
            assert(depth_[v] == naive_depth(v));
        };

        Base::on_delete_node = [&](Index const v){
            // TODO: update fast navigation
        };
    }

    // find the deepest node that spells a prefix of the input string
    Index find_prefix(StringView const& s, Index const pos, Index const len) const {
        // TODO: use fast navigation
        auto const& trie = Base::filter();
        auto v = trie.root();
        Index d = 0;
        Index child;

        while(d < len && trie.try_get_child(v, s[pos + d], child)) {
            ++d;
            v = child;
        }

        if constexpr(DEBUG) {
            std::cout << "\tfind_prefix for \"" << s.string_view().substr(pos, len) << "\" results in node " << v << " at depth " << depth(v) << std::endl;
        }
        return v;
    }

    // retrieves the depth of the given node
    Index depth(Index const v) const {
        return depth_[v];
    }

    void insert(StringView const& str, Index const pos, Index const len, Index const limit = std::numeric_limits<Index>::max()) {
        auto const max = std::min(len, limit);
        if constexpr(DEBUG) {
            ++insert_seq_;
            std::cout << "\tinsert string [seq=" << insert_seq_ << "] \"" << str.string_view().substr(pos, max) << "\"" << std::endl;
        }

        auto s = Base::empty_string();

        while((s.frequent || s.new_node) && s.len < max) {
            assert(depth_[s.node] == s.len);
            auto const c = str[pos + s.len];
            s = Base::extend(s, c, str.fingerprint(pos, pos + s.len));

            if(s.new_node) {
                if constexpr(DEBUG) std::cout << "\t\tcreated new node " << s.node << " for " << display(c) << std::endl;

                // TODO: update fast navigation
            } else {
                if constexpr(DEBUG) std::cout << "\t\tnavigated to node " << s.node << " for " << display(c) << std::endl;
            }
        }
    }
};
