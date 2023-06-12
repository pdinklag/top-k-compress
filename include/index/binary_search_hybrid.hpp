#pragma once

#include <cassert>
#include <cstdint>
#include <cstddef>

#include <concepts>

#include "result.hpp"

/// \brief Binary predecessor search that switches to linear search in small intervals.
/// \tparam key_t the key type
/// \tparam linear_threshold if the search interval becomes smaller than this, switch to linear search 
template<typename Data, size_t linear_threshold_ = 512ULL / sizeof(Data)>
class BinarySearchHybrid {
public:
    /// \brief Finds the rank of the predecessor of the specified key in the given interval.
    /// \param keys the keys
    /// \param p the left search interval border
    /// \param q the right search interval border
    /// \param x the key in question
    template<typename Key>
    inline static PosResult predecessor_seeded(Data const* data, size_t p, size_t q, const Key& x) {
        assert(p <= q);
        
        while(q - p > linear_threshold_) {
            assert(data[p] <= x);

            const size_t m = (p + q) >> 1ULL;
            const bool le = (data[m] <= x);

            /*
                the following is a fast form of:
                if(le) p = m; else q = m;
            */
            const size_t le_mask = -size_t(le);
            const size_t gt_mask = ~le_mask;

            p = (le_mask & m) | (gt_mask & p);
            q = (gt_mask & m) | (le_mask & q);
        }

        // linear search
        while(data[p] <= x) ++p;
        assert(data[p-1] <= x);

        return PosResult { true, p-1 };
    }
    
    /// \brief Finds the rank of the predecessor of the specified key.
    /// \tparam keyarray_t the key array type
    /// \param keys the keys that the compressed trie was constructed for
    /// \param num the number of keys
    /// \param x the key in question
    template<typename Key>
    inline static PosResult predecessor(Data const* data, size_t const num, const Key& x) {
        if(data[0] > x)      [[unlikely]] return PosResult { false, 0 };
        if(data[num-1] <= x) [[unlikely]] return PosResult { true, num-1 };
        return predecessor_seeded(data, 0, num-1, x);
    }

    /// \brief Finds the rank of the successor of the specified key in the given interval.
    /// \param keys the keys
    /// \param p the left search interval border
    /// \param q the right search interval border
    /// \param x the key in question
    template<typename Key>
    inline static PosResult successor_seeded(Data const* data, size_t p, size_t q, const Key& x) {
        assert(p <= q);
        
        while(q - p > linear_threshold_) {
            assert(data[q] >= x);

            const size_t m = (p + q) >> 1ULL;
            const bool le = (data[m] <= x);

            /*
                the following is a fast form of:
                if(le) p = m; else q = m;
            */
            const size_t le_mask = -size_t(le);
            const size_t gt_mask = ~le_mask;

            p = (le_mask & m) | (gt_mask & p);
            q = (gt_mask & m) | (le_mask & q);
        }

        // linear search
        while(data[q] >= x) --q;
        assert(data[q+1] >= x);

        return PosResult { true, q+1 };
    }
    
    /// \brief Finds the rank of the predecessor of the specified key.
    /// \tparam keyarray_t the key array type
    /// \param keys the keys that the compressed trie was constructed for
    /// \param num the number of keys
    /// \param x the key in question
    template<typename Key>
    inline static PosResult successor(Data const* data, size_t const num, const Key& x) {
        if(data[0] >= x)    [[unlikely]] return PosResult { true, 0 };
        if(data[num-1] < x) [[unlikely]] return PosResult { false, 0 };
        return successor_seeded(data, 0, num-1, x);
    }
};
