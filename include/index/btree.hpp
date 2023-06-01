#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>

#include "btree_node.hpp"
#include "key_result.hpp"

template<std::totally_ordered Key, size_t degree_>
class BTree {
private:
    static_assert((degree_ % 2) == 1); // we only allow odd maximum degrees for the sake of implementation simplicity
    static_assert(degree_ > 1);
    static_assert(degree_ < 65536);

    using ChildCount = typename std::conditional_t<degree_ < 256, uint8_t, uint16_t>;
    
    static constexpr size_t max_node_keys_ = degree_ - 1;
    static constexpr size_t split_right_ = max_node_keys_ / 2;
    static constexpr size_t split_mid_ = split_right_ - 1;
    static constexpr size_t deletion_threshold_ = degree_ / 2;

public:
    class Node {
    private:
        friend class BTree;
    
        BTreeSortedNodeLS<Key, max_node_keys_> impl_;
        ChildCount num_children_;
        Node** children_;
    
    public:
        inline bool is_leaf() const { return children_ == nullptr; }
        inline size_t size() const { return impl_.size(); }
        ChildCount num_children() const { return num_children_; }
        Node const* child(size_t const i) { return children_[i]; }
        
    private:
        inline bool is_empty() const { return size() == 0; }
        inline bool is_full() const { return size() == max_node_keys_; }

        Node() : children_(nullptr), num_children_(0) {
        }
        
        ~Node() {
            for(size_t i = 0; i < num_children_; i++) {
                delete children_[i];
            }
            if(children_) delete[] children_;
        }

        Node(Node const&) = default;
        Node(Node&&) = default;
        Node& operator=(Node const&) = default;
        Node& operator=(Node&&) = default;

        inline void allocate_children() {
            if(children_ == nullptr) {
                children_ = new Node*[degree_];
            }
        }

        void insert_child(size_t const i, Node* node) {
            assert(i <= num_children_);
            assert(num_children_ < degree_);
            allocate_children();
            
            // insert
            for(size_t j = num_children_; j > i; j--) {
                children_[j] = children_[j-1];
            }
            children_[i] = node;
            ++num_children_;
        }
        
        void remove_child(size_t const i) {
            assert(num_children_ > 0);
            assert(i <= num_children_);

            for(size_t j = i; j < num_children_-1; j++) {
                children_[j] = children_[j+1];
            }
            children_[num_children_-1] = nullptr;
            --num_children_;
            
            if(num_children_ == 0) {
                delete[] children_;
                children_ = nullptr;
            }
        }

        void split_child(const size_t i) {
            assert(!is_full());
            
            Node* y = children_[i];
            assert(y->is_full());

            // allocate new node
            Node* z = new Node();

            // get the middle value
            Key const m = y->impl_[split_mid_];

            // move the keys larger than middle from y to z and remove the middle
            {
                Key buf[split_right_];
                for(size_t j = 0; j < split_right_; j++) {
                    buf[j] = y->impl_[j + split_right_];
                    z->impl_.insert(buf[j]);
                }
                for(size_t j = 0; j < split_right_; j++) {
                    y->impl_.remove(buf[j]);
                }
                y->impl_.remove(m);
            }

            // move the m_children right of middle from y to z
            if(!y->is_leaf()) {
                z->allocate_children();
                for(size_t j = split_right_; j <= max_node_keys_; j++) {
                    z->children_[z->num_children_++] = y->children_[j];
                }
                y->num_children_ -= (split_right_ + 1);
            }

            // insert middle into this and add z as child i+1
            impl_.insert(m);
            insert_child(i + 1, z);

            // some assertions
            assert(children_[i] == y);
            assert(children_[i+1] == z);
            assert(z->impl_.size() == split_right_);
            if(!y->is_leaf()) assert(z->num_children_ == split_right_ + 1);
            assert(y->impl_.size() == split_mid_);
            if(!y->is_leaf()) assert(y->num_children_ == split_mid_ + 1);
        }
        
        void insert(Key const key) {
            assert(!is_full());
            
            if(is_leaf()) {
                // we're at a leaf, insert
                impl_.insert(key);
            } else {
                // find the child to descend into
                auto const r = impl_.predecessor(key);
                size_t i = r.exists ? r.pos + 1 : 0;
                
                if(children_[i]->is_full()) {
                    // it's full, split it up first
                    split_child(i);

                    // we may have to increase the index of the child to descend into
                    if(key > impl_[i]) ++i;
                }

                // descend into non-full child
                children_[i]->insert(key);
            }
        }

        bool remove(Key const key) {
            assert(!is_empty());

            if(is_leaf()) {
                // leaf - simply remove
                return impl_.remove(key);
            } else {
                // find the item, or the child to descend into
                auto const r = impl_.predecessor(key);
                size_t i = r.exists ? r.pos + 1 : 0;
                
                if(r.exists && impl_[r.pos] == key) {
                    // key is contained in this internal node
                    assert(i < degree_);

                    Node* y = children_[i-1];
                    const size_t ysize = y->size();
                    Node* z = children_[i];
                    const size_t zsize = z->size();
                    
                    if(ysize >= deletion_threshold_) {
                        // find predecessor of key in y's subtree - i.e., the maximum
                        Node* c = y;
                        while(!c->is_leaf()) {
                            c = c->children_[c->num_children_-1];
                        }
                        Key const key_pred = c->impl_[c->size()-1];

                        // replace key by predecssor in this node
                        impl_.remove(key);
                        impl_.insert(key_pred);

                        // recursively delete key_pred from y
                        y->remove(key_pred);
                    } else if(zsize >= deletion_threshold_) {
                        // find successor of key in z's subtree - i.e., its minimum
                        Node* c = z;
                        while(!c->is_leaf()) {
                            c = c->children_[0];
                        }
                        Key const key_succ = c->impl_[0];

                        // replace key by successor in this node
                        impl_.remove(key);
                        impl_.insert(key_succ);

                        // recursively delete key_succ from z
                        z->remove(key_succ);
                    } else {
                        // assert balance
                        assert(ysize == deletion_threshold_ - 1);
                        assert(zsize == deletion_threshold_ - 1);

                        // remove key from this node
                        impl_.remove(key);

                        // merge key and all of z into y
                        {
                            // insert key and keys of z into y
                            y->impl_.insert(key);
                            for(size_t j = 0; j < zsize; j++) {
                                y->impl_.insert(z->impl_[j]);
                            }

                            // move m_children from z to y
                            if(!z->is_leaf()) {
                                assert(!y->is_leaf()); // the sibling of an inner node cannot be a leaf
                                
                                size_t next_child = y->num_children_;
                                for(size_t j = 0; j < z->num_children_; j++) {
                                    y->children_[next_child++] = z->children_[j];
                                }
                                y->num_children_ = next_child;
                                z->num_children_ = 0;
                            }
                        }

                        // delete z
                        remove_child(i);
                        delete z;

                        // recursively delete key from y
                        y->remove(key);
                    }
                    return true;
                } else {
                    // get i-th child
                    Node* c = children_[i];

                    if(c->size() < deletion_threshold_) {
                        // preprocess child so we can safely remove from it
                        assert(c->size() == deletion_threshold_ - 1);
                        
                        // get siblings
                        Node* left  = i > 0 ? children_[i-1] : nullptr;
                        Node* right = i < num_children_-1 ? children_[i+1] : nullptr;
                        
                        if(left && left->size() >= deletion_threshold_) {
                            // there is a left child, so there must be a splitter
                            assert(i > 0);
                            
                            // retrieve splitter and move it into c
                            Key const splitter = impl_[i-1];
                            assert(key > splitter); // sanity
                            impl_.remove(splitter);
                            c->impl_.insert(splitter);
                            
                            // move largest key from left sibling to this node
                            Key const llargest = left->impl_[left->size()-1];
                            assert(splitter > llargest); // sanity
                            left->impl_.remove(llargest);
                            impl_.insert(llargest);
                            
                            // move rightmost child of left sibling to c
                            if(!left->is_leaf()) {
                                Node* lrightmost = left->children_[left->num_children_-1];
                                left->remove_child(left->num_children_-1);
                                c->insert_child(0, lrightmost);
                            }
                        } else if(right && right->size() >= deletion_threshold_) {
                            // there is a right child, so there must be a splitter
                            assert(i < impl_.size());
                            
                            // retrieve splitter and move it into c
                            Key const splitter = impl_[i];
                            assert(key < splitter); // sanity
                            impl_.remove(splitter);
                            c->impl_.insert(splitter);
                            
                            // move smallest key from right sibling to this node
                            Key const rsmallest = right->impl_[0];
                            assert(rsmallest > splitter); // sanity
                            right->impl_.remove(rsmallest);
                            impl_.insert(rsmallest);
                            
                            // move leftmost child of right sibling to c
                            if(!right->is_leaf()) {
                                Node* rleftmost = right->children_[0];
                                right->remove_child(0);
                                c->insert_child(c->num_children_, rleftmost);
                            }
                        } else {
                            // this node is not a leaf and is not empty, so there must be at least one sibling to the child
                            assert(left != nullptr || right != nullptr);
                            assert(left == nullptr || left->size() == deletion_threshold_ - 1);
                            assert(right == nullptr || right->size() == deletion_threshold_ - 1);
                            
                            // select the sibling and corresponding splitter to mergre with
                            if(right != nullptr) {
                                // merge child with right sibling
                                Key const splitter = impl_[i];
                                assert(key < splitter); // sanity
                                
                                // move splitter into child as new median
                                impl_.remove(splitter);
                                c->impl_.insert(splitter);
                                
                                // move keys right sibling to child
                                for(size_t j = 0; j < right->size(); j++) {
                                    c->impl_.insert(right->impl_[j]);
                                }
                                
                                if(!right->is_leaf()) {
                                    assert(!c->is_leaf()); // the sibling of an inner node cannot be a leaf
                                    
                                    // append m_children of right sibling to child
                                    size_t next_child = c->num_children_;
                                    for(size_t j = 0; j < right->num_children_; j++) {
                                        c->children_[next_child++] = right->children_[j];
                                    }
                                    c->num_children_ = next_child;
                                    right->num_children_ = 0;
                                }
                                
                                // delete right sibling
                                remove_child(i+1);
                                delete right;
                            } else {
                                // merge child with left sibling
                                Key const splitter = impl_[i-1];
                                assert(key > splitter); // sanity
                                
                                // move splitter into child as new median
                                impl_.remove(splitter);
                                c->impl_.insert(splitter);
                                
                                // move keys left sibling to child
                                for(size_t j = 0; j < left->size(); j++) {
                                    c->impl_.insert(left->impl_[j]);
                                }
                                
                                if(!left->is_leaf()) {
                                    assert(!c->is_leaf()); // the sibling of an inner node cannot be a leaf
                                    
                                    // move m_children of child to the back
                                    size_t next_child = left->num_children_;
                                    for(size_t j = 0; j < c->num_children_; j++) {
                                        c->children_[next_child++] = c->children_[j];
                                    }
                                    c->num_children_ = next_child;
                                    
                                    // prepend m_children of left sibling to child
                                    for(size_t j = 0; j < left->num_children_; j++) {
                                        c->children_[j] = left->children_[j];
                                    }
                                    left->num_children_ = 0;
                                }
                                
                                // delete left sibling
                                remove_child(i-1);
                                delete left;
                            }
                        }
                    }
                    
                    // remove from subtree
                    return c->remove(key);
                }
            }
        }
    } __attribute__((__packed__));

private:
    size_t size_;
    Node* root_;

public:
    BTree() : size_(0), root_(new Node()) {
    }

    ~BTree() {
        delete root_;
    }

    KeyResult<Key> predecessor(Key const x) const {
        Node* node = root_;
        
        bool exists = false;
        Key value;
        
        auto r = node->impl_.predecessor(x);
        while(!node->is_leaf()) {
            exists = exists || r.exists;
            if(r.exists) {
                value = node->impl_[r.pos];
                if(value == x) {
                    return { true, value };
                }
            }
               
            size_t const i = r.exists ? r.pos + 1 : 0;
            node = node->children_[i];
            r = node->impl_.predecessor(x);
        }
        
        exists = exists || r.exists;
        if(r.exists) value = node->impl_[r.pos];
        
        return { exists, value };
    }

    KeyResult<Key> successor(Key const x) const {
        Node* node = root_;
        
        bool exists = false;
        Key value;
        
        auto r = node->impl_.successor(x);
        while(!node->is_leaf()) {
            exists = exists || r.exists;
            if(r.exists) {
                value = node->impl_[r.pos];
                if(value == x) {
                    return { true, value };
                }
            }

            size_t const i = r.exists ? r.pos : node->num_children_ - 1;
            node = node->children_[i];
            r = node->impl_.successor(x);
        }
        
        exists = exists || r.exists;
        if(r.exists) value = node->impl_[r.pos];
        
        return { exists, value };
    }

    bool contains(Key const x) const {
        if(size() == 0) [[unlikely]] return false;
        auto r = predecessor(x);
        return r.exists && r.key == x;
    }

    Key min() const {
        Node* node = root_;
        while(!node->is_leaf()) {
            node = node->children_[0];
        }
        return node->impl_[0];
    }

    Key max() const {
        Node* node = root_;
        while(!node->is_leaf()) {
            node = node->children_[node->num_children_ - 1];
        }
        return node->impl_[node->size() - 1];
    }

    void insert(Key const key) {
        if(root_->is_full()) {
            // root is full, split it up
            Node* new_root = new Node();
            new_root->insert_child(0, root_);

            root_ = new_root;
            root_->split_child(0);
        }
        root_->insert(key);
        ++size_;
    }

    bool remove(Key const key) {
        assert(size_ > 0);
        
        bool const result = root_->remove(key);
        
        if(result) {
            --size_;
        }
        
        if(root_->size() == 0 && root_->num_children_ > 0) {
            assert(root_->num_children_ == 1);
            
            // root is now empty but it still has a child, make that new root
            Node* new_root = root_->children_[0];
            root_->num_children_ = 0;
            delete root_;
            root_ = new_root;
        }
        return result;
    }

    void clear() {
        delete root_;
        root_ = new Node();
        size_ = 0;
    }

    size_t size() const { return size_; }
};
