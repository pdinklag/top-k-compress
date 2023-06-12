#pragma once

struct PosResult {
    bool exists;
    size_t pos;
    
    inline static PosResult none() { return { false, 0 }; }
};

template<typename Key, typename Value>
struct KeyValueResult {
    bool exists;
    Key key;
    Value value;

    inline static KeyValueResult none() { return { false, Key(), Value() }; }
};
