#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <functional>
#include <iostream>
#include <memory>

#include "always_inline.hpp"

template<typename T>
concept RankedLinkedListItem =
    requires { typename T::Index; } &&
    requires(T const& item) {
        { item.rank() } -> std::convertible_to<typename T::Index>;
        { item.prev() } -> std::convertible_to<typename T::Index>;
        { item.next() } -> std::convertible_to<typename T::Index>;
    } &&
    requires(T& item, typename T::Index const x) {
        { item.rank(x) };
        { item.prev(x) };
        { item.next(x) };
    };

template<RankedLinkedListItem T>
class RankedLinkedList {
private:
    using Index = typename T::Index;

    static constexpr Index NIL = -1;

    Index head_;

    void verify(T const* items) const {
        #ifndef NDEBUG
        Index count = 0;
        auto prev = NIL;
        for(auto cur = head_; cur != NIL; cur = items[cur].next()) {
            assert(cur != prev);
            ++count;
            
            if(prev != NIL) {
                auto prev_rank = items[prev].rank();
                auto cur_rank = items[cur].rank();
                assert(cur_rank == prev_rank - 1);
            }

            assert(items[cur].prev() == prev);
            prev = cur;
        }

        assert(count == size(items));
        #endif
    }

public:
    RankedLinkedList() : head_(NIL) {
    }

    RankedLinkedList(RankedLinkedList&&) = default;
    RankedLinkedList& operator=(RankedLinkedList&&) = default;

    RankedLinkedList(RankedLinkedList const& other) = default;
    RankedLinkedList& operator=(RankedLinkedList const& other) = default;

    void push_front(T* items, Index const i) {
        auto& item = items[i];

        Index rank;
        if(head_ != NIL) {
            auto& head = items[head_];
            rank = head.rank() + 1;
            head.prev(i);
        } else {
            rank = 0;
        }

        item.prev(NIL);
        item.next(head_);
        item.rank(rank);

        head_ = i;

        #ifndef NDEBUG
        verify(items);
        assert(contains(items, i));
        #endif
    }

    void pop_front(T* items) {
        erase(head_, items);
    }

    void erase(T* items, Index const i) {
        assert(!empty());

        auto& item_to_delete = items[i];
        if(i != head_) {
            // we want to make sure that the item to be removed is the head
            // we can achieve that by swapping
            auto const iprev = item_to_delete.prev();
            auto const inext = item_to_delete.next();

            assert(iprev != NIL); // item to delete cannot be the head, we checked that
            
            auto& former_head = items[head_];

            auto const hnext = former_head.next();
            assert(hnext != NIL); // head must have a next item, otherwise item to delete could not be linked
            item_to_delete.prev(NIL); // item to delete will now be the head
            if(hnext != i) {
                item_to_delete.next(hnext);
            } else {
                // nb: if item to delete is successor of head, ensure that it doesn't link to itself!
                item_to_delete.next(head_);
            }

            // put former head at the position of the item to delete
            assert(items[iprev].next() == i);
            if(iprev == head_) {
                former_head.prev(NIL);
            } else {
                former_head.prev(iprev);
                items[iprev].next(head_); //nb: same here
            }
            
            former_head.next(inext);
            if(inext != NIL) {
                assert(items[inext].prev() == i);
                items[inext].prev(head_);
            }
            
            // finally, assume its rank
            former_head.rank(item_to_delete.rank());

            // FIXME: after this, the previous pointer of an the former head can point to itself -- how?
        }
        
        // remove the head and make the next item the new head, preserving all ranks
        // assert(item_to_delete.prev() == NIL); // nb: commented because we don't enforce this

        head_ = item_to_delete.next();
        if(head_ != NIL) items[head_].prev(NIL);

        #ifndef NDEBUG
        verify(items);
        assert(!contains(items, i));
        #endif
    }

    void append(T* items, RankedLinkedList<T> const& other) {
        if(empty()) {
            // trivial
            *this = other;
        } else {
            // compute total size
            auto const total_size = size(items) + other.size(items);
            
            // find tail and adjust ranks on the way
            auto rank = total_size;
            Index last;
            for(auto x = head_; x != NIL; last = x, x = items[x].next()) {
                assert(rank > 0);
                items[x].rank(--rank);
            }

            // link tail to head of other
            auto& tail = items[last];
            tail.next(other.front());

            if(other.front() != NIL) {
                auto& link = items[other.front()];
                link.prev(last);
                assert(link.rank() == rank);
            }

            #ifndef NDEBUG
            verify(items);
            assert(size(items) == total_size);
            #endif
        }
    }

    void clear() {
        head_ = NIL; // well ...
    }

    bool contains(T const* items, Index const i) const {
        for(auto cur = head_; cur != NIL; cur = items[cur].next()) {
            if(cur == i) return true;
        }
        return false;
    }

    Index front() const { return head_; }
    size_t size(T const* items) const { return head_ != NIL ? items[head_].rank() + 1 : 0; }
    bool empty() const { return head_ == NIL; }
} __attribute__((packed));
