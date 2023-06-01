#pragma once

template<typename Key>
struct KeyResult {
    bool exists;
    Key key;
    
    inline operator bool() const { return exists; }
    inline operator Key() const { return key; }
};
