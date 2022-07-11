#include <cstdint>

inline uintmax_t fnv(uintmax_t key, uintmax_t offset, uintmax_t prime, uintmax_t bits_per_character) {
    uintmax_t hash = offset;
    while(key) {
        hash ^= key;
        hash *= prime;
        key >>= bits_per_character;
    }
    return hash;
}
