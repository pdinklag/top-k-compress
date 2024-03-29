#pragma once

#include <cassert>
#include <concepts>
#include <cstddef>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

// a pool of linked lists sharing the same allocated memory area, in the hope to reduce cache misses in practice
template<typename Item, std::unsigned_integral EntryIndex = uint32_t>
class ListPool {
public:
    static constexpr EntryIndex NIL = std::numeric_limits<EntryIndex>::max();

    struct Entry {
        EntryIndex prev;
        EntryIndex next;
        Item item;
    };

private:
    std::unique_ptr<Entry[]> data_;
    std::vector<EntryIndex> free_;

public:
    ListPool(size_t const max_items) {
        data_ = std::make_unique<Entry[]>(max_items);

        free_.reserve(max_items);
        for(size_t i = 0; i < max_items; i++) {
            free_.push_back(max_items - 1 - i); // nb: fill in reverse, so front indices are used first
        }
    }

    // claim an entry from the pool
    EntryIndex claim() {
        assert(!free_.empty());

        auto const i = free_.back();
        free_.pop_back();
        return i;
    }

    // release an entry back into the pool
    void release(EntryIndex const i) {
        free_.push_back(i);
    }

    Entry& entry(EntryIndex const e) { assert(e != NIL); return data_[e]; }
    
    Entry const& entry(EntryIndex const e) const { assert(e != NIL); return data_[e]; }

    size_t capacity() const { return free_.capacity(); }

    struct Iterator {
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type   = ssize_t;
        using value_type        = Item;
        using pointer           = Item*;
        using reference         = Item&;

        ListPool* pool;
        EntryIndex entry;

        Iterator() : pool(nullptr), entry(NIL) {}
        Iterator(ListPool& _pool, EntryIndex const _entry) : pool(&_pool), entry(_entry) {}

        Iterator(Iterator const&) = default;
        Iterator& operator=(Iterator const&) = default;
        Iterator(Iterator&&) = default;
        Iterator& operator=(Iterator&&) = default;

        bool operator==(Iterator const& other) const { return pool == other.pool && entry == other.entry; }
        bool operator!=(Iterator const& other) const { return !(*this == other); }

        Iterator& operator++() { entry = pool->entry(entry).next; return *this; }
        auto operator++(int) { auto cpy = *this; ++(*this); return cpy; }
        Iterator& operator--() { entry = pool->entry(entry).prev; return *this; }
        auto operator--(int) { auto cpy = *this; --(*this); return cpy; }

        Item& operator*() const { return pool->entry(entry).item; }
        Item* operator->() const { return &pool->entry(entry).item; }
    } __attribute__((packed));
    
    struct ConstIterator {
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type   = ssize_t;
        using value_type        = Item;
        using pointer           = Item const*;
        using reference         = Item const&;

        ListPool const* pool;
        EntryIndex entry;

        ConstIterator() : pool(nullptr), entry(NIL) {}
        ConstIterator(ListPool const& _pool, EntryIndex const _entry) : pool(&_pool), entry(_entry) {}

        ConstIterator(ConstIterator const&) = default;
        ConstIterator& operator=(ConstIterator const&) = default;
        ConstIterator(ConstIterator&&) = default;
        ConstIterator& operator=(ConstIterator&&) = default;

        bool operator==(ConstIterator const& other) const { return pool == other.pool && entry == other.entry; }
        bool operator!=(ConstIterator const& other) const { return !(*this == other); }

        ConstIterator& operator++() { entry = pool->entry(entry).next; return *this; }
        auto operator++(int) { auto cpy = *this; ++(*this); return cpy; }
        ConstIterator& operator--() { entry = pool->entry(entry).prev; return *this; }
        auto operator--(int) { auto cpy = *this; --(*this); return cpy; }

        Item const& operator*() const { return pool->entry(entry).item; }
        Item const* operator->() const { return &pool->entry(entry).item; }
    } __attribute__((packed));

    class List {
    private:
        ListPool* pool_;
        EntryIndex head_;
        EntryIndex tail_;
        EntryIndex size_;

        template<typename It>
        std::pair<EntryIndex, Entry*> insert(It pos) {
            auto const i = pool_->claim();
            auto& e = pool_->entry(i);

            if(pos.entry == NIL) {
                // insert at end
                e.prev = tail_;
                e.next = NIL;

                if(tail_ != NIL) pool_->entry(tail_).next = i;
                tail_ = i;
            } else {
                // insert before pos
                auto& x = pool_->entry(pos.entry);
                e.prev = x.prev;
                e.next = pos.entry;

                x.prev = i;
                if(e.prev != NIL) pool_->entry(e.prev).next = i;
            }

            // possibly make head
            if(e.prev == NIL) {
                assert(pos.entry == head_);
                head_ = i;
            }

            ++size_;
            return { i, &e };
        }

    public:
        using value_type = Item;
        using size_type = size_t;
        using difference_type = void;
        using reference = Item&;
        using const_reference = Item const&;
        using pointer = Item*;
        using const_pointer = Item const*;
        using iterator = Iterator;
        using const_iterator = ConstIterator;
        using reverse_iterator = std::reverse_iterator<Iterator>;
        using const_reverse_iterator = std::reverse_iterator<ConstIterator>;

        List() : pool_(nullptr), head_(NIL), tail_(NIL), size_(0) {
        }

        List(ListPool& pool) : pool_(&pool), head_(NIL), tail_(NIL), size_(0) {
        }

        List(List const&) = delete;
        List& operator=(List const&) = delete;
        List(List&&) = default;
        List& operator=(List&&) = default;

        Item& front() { return pool_->entry(head_).item; }
        Item const& front() const { return pool_->entry(head_).item; }
        Item& back() { return pool_->entry(tail_).item; }
        Item const& back() const { return pool_->entry(tail_).item; }

        auto begin() { return Iterator(*pool_, head_); }
        auto end() { return Iterator(*pool_, NIL); }

        auto cbegin() const { return ConstIterator(*pool_, head_); }
        auto cend() const { return ConstIterator(*pool_, NIL); }

        auto rbegin() { return std::reverse_iterator(Iterator(*pool_, tail_)); }
        auto rend() { return std::reverse_iterator(Iterator(*pool_, NIL)); }

        auto crbegin() const { return std::reverse_iterator(ConstIterator(*pool_, tail_)); }
        auto crend() const { return std::reverse_iterator(ConstIterator(*pool_, NIL)); }

        bool empty() const { return size_ == 0; }
        size_t size() const { return size_; }
        size_t max_size() const { return pool_->capacity(); }

        void clear() {
            // release all entries
            for(auto it : *this) {
                pool_->release(it.entry);
            }

            // invalidate
            head_ = NIL,
            tail_ = NIL;
            size_ = 0;
        }

        template<typename It>
        Iterator insert(It pos, Item const& item) {
            auto [i,e] = insert(pos);
            e->item = item;
            return Iterator(*pool_, i);
        }

        template<typename It, typename... Args>
        Iterator emplace(It pos, Args&&... args) {
            auto [i,e] = insert(pos);
            e->item = Item(std::forward<Args>(args)...);
            return Iterator(*pool_, i);
        }

        template<typename It>
        Iterator erase(It pos) {
            auto const i = pos.entry;
            auto& e = pool_->entry(i);
            
            // erase from list
            auto const prev = e.prev;
            auto* const pprev = (prev != NIL) ? &pool_->entry(e.prev) : nullptr;
            auto const next = e.next;
            auto* const pnext = (next != NIL) ? &pool_->entry(e.next) : nullptr;

            if(pprev) {
                // remove non-head
                assert(head_ != i);
                
                pprev->next = next;
                if(pnext) pnext->prev = prev;
            } else {
                // remove head
                assert(head_ == i);
                
                head_ = next;
                if(pnext) pnext->prev = NIL;
            }

            if(pnext) {
                // remove non-tail
                assert(tail_ != i);

                pnext->prev = prev;
                if(pprev) pprev->next = next;
            } else {
                // remove tail
                assert(tail_ == i);

                tail_ = prev;
                if(pprev) pprev->next = NIL;
            }

            // release from pool
            pool_->release(i);
            --size_;

            // return next
            return Iterator(*pool_, next);
        }

        template<typename It>
        void push_back(Item const& item) {
            insert(end(), item);
        }

        void emplace_back(Item&& item) {
            emplace(end(), std::move(item));
        }

        template<typename... Args>
        void emplace_back(Args&&... args) {
            emplace(end(), std::forward<Args>(args)...);
        }

        void pop_back() {
            erase(rbegin());
        }

        void push_front(Item const& item) {
            insert(begin(), item);
        }

        void emplace_front(Item&& item) {
            emplace(begin(), std::move(item));
        }
        
        template<typename... Args>
        void emplace_front(Args&&... args) {
            emplace(begin(), std::forward<Args>(args)...);
        }

        void pop_front() {
            erase(begin());
        }
    } __attribute__((packed));

    List new_list() { return List(*this); }
};
