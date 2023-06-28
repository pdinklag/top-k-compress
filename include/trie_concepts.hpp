#pragma once

#include <concepts>

template<typename Node>
concept trie_node =
    requires {
        typename Node::Character;
        typename Node::Index;
    }
    && std::constructible_from<Node> // default
    && std::constructible_from<Node, typename Node::Index, typename Node::Character> // parent / label
    && requires(Node node) {
        { node.children };
        { node.inlabel };
        { node.parent };
    } && requires(Node const& node) {
        { node.size() } -> std::unsigned_integral;
        { node.is_leaf() } -> std::same_as<bool>;
    };
