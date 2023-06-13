#pragma once

#include <ankerl/unordered_dense.h>

template<typename Key, typename Value>
size_t memory_size_of(ankerl::unordered_dense::map<Key, Value> const& map) {
    using Map = ankerl::unordered_dense::map<Key, Value>;

    size_t mem = 0;
    mem += map.bucket_count() * sizeof(typename Map::bucket_type);
    mem += map.values().capacity() * sizeof(typename  Map::value_container_type::value_type);
    return map.size() * (sizeof(Key) + sizeof(Value));
}
