#pragma once

template<typename Key, typename Value>
struct KeyValueResult {
    bool exists;
    Key key;
    Value value;

    inline static KeyValueResult none() { return { false, Key(), Value() }; }
};
