/**
 * Copyright (C) 2018 Dean De Leo, email: dleo[at]cwi.nl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef GENERIC_DYNAMIC_INDEX_HPP_
#define GENERIC_DYNAMIC_INDEX_HPP_

#include <cassert>
#include <cinttypes>
#include <cstdlib> // posix_memalign
#include <iomanip>
#include <iostream>
#if defined(HAVE_LIBNUMA)
#include <numa.h> // to dump statistics about the memory usage
#include <numaif.h>
#include <sched.h>
#endif
#include <stdexcept>

#include "miscellaneous.hpp" // to_string_with_unit_suffix

namespace pma {

/**
 * Generic implementation of an (a,b)-tree, with separator keys, for keys and values of type K and V.
 * The maximum number of separator keys in the intermediate nodes is `inode_b', while `leaf_b'
 * is the maximum capacity, in terms of number of elements, in the leaves.
 */
template<typename K, typename V, int inode_b = 64, int leaf_b = 64>
class DynamicIndex {
    DynamicIndex(const DynamicIndex&) = delete;
    DynamicIndex& operator= (DynamicIndex&) = delete;

    // The format of the internal nodes
    struct Node {
        // remove the ctors
        Node() = delete;
        Node(const Node&) = delete;
        Node& operator=(const Node&) = delete;

        size_t N;
        bool empty() const;
    };
    struct InternalNode : public Node {
        // remove the ctors
        InternalNode() = delete;
        InternalNode(const InternalNode&) = delete;
        InternalNode& operator=(const InternalNode&) = delete;
//        int64_t* keys() const;
//        Node** children() const;
    };
    K* KEYS(const InternalNode* inode) const;
    Node** CHILDREN(const InternalNode* inode) const;
    struct Leaf : public Node {
        // remove the ctors
        Leaf() = delete;
        Leaf(const Leaf&) = delete;
        Leaf& operator=(const Leaf&) = delete;

        Leaf* next;
        Leaf* previous;

//        int64_t* keys() const;
//        int64_t* values() const;
    };
    K* KEYS(const Leaf* leaf) const;
    V* VALUES(const Leaf* leaf) const;

    Node* m_root; // current root
    uint64_t m_cardinality; // number of elements inside the B-Tree
    int m_height =1; // number of levels, or height of the tree
    mutable uint64_t m_num_inodes; // number of internal nodes allocated, to compute the memory footprint
    mutable uint64_t m_num_leaves; // number of leaves allocated, to compute the memory footprint

    // Create a new node / leaf
    InternalNode* create_inode() const;
    Leaf* create_leaf() const;

    // Get the memory size of an internal node / leaf
    constexpr size_t memsize_inode() const;
    constexpr size_t memsize_leaf() const;

    // Retrieve the minimum capacity of the nodes at depth `depth' (depth starts from 0).
    size_t lowerbound(int depth) const;

    // Delete an existing node / leaf
    void delete_node(Node* node, int depth) const;

    // It splits the child of `node' at index `child' in half and adds the new node as a new child of `node'.
    void split(InternalNode* inode, size_t child_index, int child_depth);

    // It increases the height of tree by 1, by splitting the current root in half and introducing a new root.
    void split_root();

    // Insert the given key/value in the subtree rooted at the given `node'.
    void insert(Node* node, const K key, const V& value, int depth);

    // Rebalance the given inode by a series of merge/share operations
    void merge(InternalNode* node, size_t child_index, int child_depth);
    void rotate_left(InternalNode* node, size_t child_index, int child_depth, size_t num_nodes);
    void rotate_right(InternalNode* node, size_t child_index, int child_depth, size_t num_nodes);
    void rebalance_lb(InternalNode* node, size_t child_index, int child_depth);
    void rebalance_rec(Node* node, const K range_min, const K range_max, int depth);

    // Attempt to reduce the height of the tree, checking whether the root has only one child.
    bool reduce_tree();

    // It removes the key/values in the interval [range_min, range_max] for the subtree rooted
    // at the given `node'. It returns `true' if any of the nodes in the given subtree does not
    // respect the constraint of their size in [A, B], false otherwise.
    // The parameter `min' is an output variable to return the minimum value in the subtree.
    bool remove_keys(Node* node, const K range_min ,const K range_max, int depth, bool* min_set, K* min_key);

    // Remove the children of node in the interval [index, index + length).
    void remove_subtrees(InternalNode* node, size_t index, size_t length, int children_depth);

    // Helper method, it performs the recursion of remove_subtrees
    void remove_subtrees_rec0(Node* node, int depth);

    // Remove the given interval from the sub-tree starting at node
    void remove(Node* node, const K keymin, const K range_max, int depth);

    // Remove a single element from the tree. Return true if an element has been set and the argument value is set to the value of that element
    bool remove_any(Node* node, const K key, V* out_value, int depth, K* omin_key);

    // Dump the content of the B-Tree in the given output stream
    void dump_data(std::ostream&, Node* node, int depth) const;

    // Dump the memory usage of the B-Tree in the given output stream
    void dump_memory(std::ostream&) const;

public:
    /**
     * Constructor
     */
    DynamicIndex();

    /**
     * Destructor
     */
    ~DynamicIndex();

    /**
     * Insert the given pair in the index
     */
    void insert(const K key, const V& value);

    /**
     * Remove all elements with the given `key'
     */
    void remove(const K key);

    /**
     * Remove an element from the tree `key':
     * - If an element has been removed, return true and set `value' to its payload
     * - Otherwise, return false.
     * In presence of duplicates, the implementation non deterministically picks one of the elements to remove.
     */
    bool remove_any(const K key, V* out_value = nullptr);
    /**
     * Find the the element with the given `key'. In case of duplicates, the implementation selects one of elements non deterministically.
     * Return `true' if found, `false' otherwise.
     */
    bool find_any(const K key, V* output_value) const;

    /**
     * Return the first entry less or equal than the given key.
     */
    bool find_first(const K key, K* output_key, V* output_value) const;

    /**
     * Return the last entry greater or equal than the given key.
     */
    bool find_last(const K key, K* output_key, V* output_value) const;

    /**
     * Retrieve the current cardinality of the index
     */
    uint64_t size() const;

    /**
     * Check whether the index is empty
     */
    bool empty() const;

    /**
     * Remove all entries in the index
     */
    void clear();

    /**
     * Retrieve the memory footprint of this index, in bytes
     */
    uint64_t memory_footprint() const;

    /**
     * Dump the content of the index
     */
    void dump() const;
    void dump(std::ostream& out) const;
};

/*****************************************************************************
 *                                                                           *
 *   Implementation                                                          *
 *                                                                           *
 *****************************************************************************/

template<typename K, typename V, int inode_b, int leaf_b>
DynamicIndex<K, V, inode_b, leaf_b>::DynamicIndex() :
    m_root(nullptr), m_cardinality(0), m_num_inodes(0), m_num_leaves(0){
    m_root = create_leaf();
}

template<typename K, typename V, int inode_b, int leaf_b>
DynamicIndex<K, V, inode_b, leaf_b>::~DynamicIndex() {
    delete_node(m_root, 0); m_root = nullptr;
}

template<typename K, typename V, int inode_b, int leaf_b>
typename DynamicIndex<K, V, inode_b, leaf_b>::Leaf* DynamicIndex<K, V, inode_b, leaf_b>::create_leaf() const {
    static_assert(!std::is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + ptr previous + ptr next");

    // (cardinality) 1 + (ptr left/right) 2 + (keys=) leaf_b + (values) leaf_b == 2 * leaf_b + 1;
    Leaf* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_leaf());
    if(rc != 0) throw std::runtime_error("DynamicIndex::create_leaf, cannot obtain a chunk of aligned memory");
    ptr->N = 0;
    ptr->next = ptr->previous = nullptr;

    m_num_leaves++;
    return ptr;
}

template<typename K, typename V, int inode_b, int leaf_b>
typename DynamicIndex<K, V, inode_b, leaf_b>::InternalNode* DynamicIndex<K, V, inode_b, leaf_b>::create_inode() const {
    static_assert(!std::is_polymorphic<InternalNode>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(InternalNode) == 8, "Expected only 8 bytes for the cardinality");

    // (cardinality) 1 + (keys=) intnode_b + (pointers) intnode_b +1 == 2 * intnode_b +2;
    InternalNode* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_inode());
    if(rc != 0) throw std::runtime_error("ABTree::create_internal_node, cannot obtain a chunk of aligned memory");
    ptr->N = 0;

    m_num_inodes++;
    return ptr;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::delete_node(Node* node, int depth) const {
    assert(node != nullptr);
    bool is_leaf = (depth == m_height -1);

    if(!is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);

        for(size_t i = 0; i < inode->N; i++){
            delete_node(children[i], depth +1);
        }

        assert(m_num_inodes > 0 && "Underflow");
        m_num_inodes--;
    } else {
        assert(m_num_leaves > 0 && "Underflow");
        m_num_leaves--;
    }

    free(node);
}

template<typename K, typename V, int inode_b, int leaf_b>
constexpr size_t DynamicIndex<K, V, inode_b, leaf_b>::memsize_inode() const {
    return sizeof(InternalNode) + sizeof(K) * inode_b + sizeof(Node*) * (inode_b +1);
}

template<typename K, typename V, int inode_b, int leaf_b>
constexpr size_t DynamicIndex<K, V, inode_b, leaf_b>::memsize_leaf() const {
    return sizeof(Leaf) + leaf_b * (sizeof(K) + sizeof(V));
}

template<typename K, typename V, int inode_b, int leaf_b>
size_t DynamicIndex<K, V, inode_b, leaf_b>::lowerbound(int depth) const{
    bool is_leaf = (depth == m_height -1);
    return is_leaf ? leaf_b/2 : inode_b/2;
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::Node::empty() const {
    return N == 0;
}
template<typename K, typename V, int inode_b, int leaf_b>
K* DynamicIndex<K, V, inode_b, leaf_b>::KEYS(const InternalNode* inode) const{
    InternalNode* instance = const_cast<InternalNode*>(inode);
    return reinterpret_cast<K*>(reinterpret_cast<uint8_t*>(instance) + sizeof(InternalNode));
}
template<typename K, typename V, int inode_b, int leaf_b>
typename DynamicIndex<K, V, inode_b, leaf_b>::Node** DynamicIndex<K, V, inode_b, leaf_b>::CHILDREN(const InternalNode* inode) const {
    return reinterpret_cast<Node**>(KEYS(inode) + inode_b);
}
template<typename K, typename V, int inode_b, int leaf_b>
K* DynamicIndex<K, V, inode_b, leaf_b>::KEYS(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<K*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}
template<typename K, typename V, int inode_b, int leaf_b>
V* DynamicIndex<K, V, inode_b, leaf_b>::VALUES(const Leaf* leaf) const {
    return reinterpret_cast<V*>(KEYS(leaf) + leaf_b);
}
template<typename K, typename V, int inode_b, int leaf_b>
uint64_t DynamicIndex<K, V, inode_b, leaf_b>::size() const {
    return m_cardinality;
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::empty() const {
    return size() == 0;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::clear() {
    delete_node(m_root, 0); m_root = nullptr;
    assert(m_num_inodes == 0);
    assert(m_num_leaves == 0);
    m_root = create_leaf();
    m_cardinality = 0;
    m_height = 1;
}

template<typename K, typename V, int inode_b, int leaf_b>
uint64_t DynamicIndex<K, V, inode_b, leaf_b>::memory_footprint() const {
    return sizeof(*this) + m_num_inodes * memsize_inode() + m_num_leaves * memsize_leaf();
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::insert(const K key, const V& value){
    // split the root when it is a leaf
    if(m_height == 1 && m_root->N == leaf_b){
        split_root();
    }

    insert(m_root, key, value, 0);

    // split the root when it is an internal node
    if(m_height > 1 && m_root->N > inode_b){
        split_root();
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::insert(Node* node, const K key, const V& value, int depth){
    assert(node != nullptr);

    // tail recursion on the internal nodes
    while(depth < (m_height -1)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        assert(inode->N > 0);
        size_t i = 0, last_key = inode->N -1;
        K* __restrict keys = KEYS(inode);
        while(i < last_key && key > keys[i]) i++;
        node = CHILDREN(inode)[i];

        // before moving to its child, check whether it is full. If this is the case
        // we need to make a recursive call to check again whether we need to split the
        // node after an element has been inserted
        bool child_is_leaf = (depth + 1) >= m_height -1;
        if(child_is_leaf && node->N == leaf_b){
            split(inode, i, depth +1); // we already know we are going to insert an element
            if(key > KEYS(inode)[i]) node = CHILDREN(inode)[++i];
        } else if (!child_is_leaf && node->N == inode_b){
            insert(node, key, value, depth+1);
            if(node->N > inode_b){ split(inode, i, depth+1); }
            return; // stop the loop
        }

        depth++;
    }

    // finally, shift the elements & insert into the leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N < leaf_b);
    size_t i = leaf->N;
    K* __restrict keys = KEYS(leaf);
    V* __restrict values = VALUES(leaf);
    while(i > 0 && keys[i-1] > key){
        keys[i] = keys[i-1];
        values[i] = values[i-1];
        i--;
    }
    keys[i] = key;
    values[i] = value;
    leaf->N++;

    m_cardinality += 1;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::split_root(){
    InternalNode* root0 = create_inode();
    CHILDREN(root0)[0] = m_root;
    root0->N = 1;
    m_height++;
    split(root0, 0, 1);
    m_root = root0;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::split(InternalNode* inode, size_t child_index, int child_depth){
    assert(inode != nullptr);
    assert(child_index <= inode->N);

    bool child_is_leaf = child_depth >= m_height -1;
    K pivot {};
    Node* ptr = nullptr; // the new child

    if(child_is_leaf){
        // split a leaf in half
        Leaf* l1 = reinterpret_cast<Leaf*>(CHILDREN(inode)[child_index]);
        Leaf* l2 = create_leaf();

        assert(l1->N <= leaf_b);

        size_t thres = (l1->N +1) /2;
        l2->N = l1->N - thres;
        assert(l2->N >= leaf_b/2);
        l1->N = thres;
        assert(l1->N >= leaf_b/2);

        // move the elements from l1 to l2
        memcpy(KEYS(l2), KEYS(l1) + thres, l2->N * sizeof(KEYS(l2)[0]));
        memcpy(VALUES(l2), VALUES(l1) + thres, l2->N * sizeof(VALUES(l2)[0]));

        // adjust the links
        l2->next = l1->next;
        if( l2->next != nullptr ) { l2->next->previous = l2; }
        l2->previous = l1;
        l1->next = l2;

        // threshold derives the new pivot
        pivot = KEYS(l2)[0]; // == l1->keys[thres]
        ptr = l2;
    }

    // split an internal node
    else {
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(inode)[child_index]);
        InternalNode* n2 = create_inode();

        size_t thres = n1->N /2;
        n2->N = n1->N - (thres +1);
        assert(n2->N >= inode_b/2);
        n1->N = thres +1;
        assert(n1->N >= inode_b/2);

        // move the elements from n1 to n2
        assert(n2->N > 0);
        memcpy(KEYS(n2), KEYS(n1) + thres + 1, (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(CHILDREN(n2), CHILDREN(n1) + thres + 1, n2->N * sizeof(CHILDREN(n2)[0]));

        // derive the new pivot
        pivot = KEYS(n1)[thres];
        ptr = n2;
    }

    // finally, add the pivot to the parent (current node)
    assert(inode->N <= inode_b); // when inserting, the parent is allowed to become b+1
    K* keys = KEYS(inode);
    Node** children = CHILDREN(inode);

    for(int64_t i = static_cast<int64_t>(inode->N) -1, child_index_signed = child_index; i > child_index_signed; i--){
        keys[i] = keys[i-1];
        children[i +1] = children[i];
    }

    keys[child_index] = pivot;
    children[child_index +1] = ptr;
    inode->N++;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::merge(InternalNode* node, size_t child_index, int child_depth){
    assert(node != nullptr);
    assert(child_index +1 <= node->N);
    bool child_is_leaf = child_depth >= m_height -1;

    // merge two adjacent leaves
    if(child_is_leaf){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];
        assert(l1->N + l2->N <= leaf_b);

        // move all elements from l2 to l1
        memcpy(KEYS(l1) + l1->N, KEYS(l2), l2->N * sizeof(KEYS(l2)[0]));
        memcpy(VALUES(l1) + l1->N, VALUES(l2), l2->N * sizeof(VALUES(l2)[0]));

        // update the sizes of the two leaves
        l1->N += l2->N;
        l2->N = 0;

        // adjust the links
        l1->next = l2->next;
        if(l2->next != nullptr){ l2->next->previous = l1; }

        // free the memory from l2
        delete_node(l2, child_depth); l2 = nullptr;
    }

    // merge two adjacent internal nodes
    else {
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(node)[child_index]);
        InternalNode* n2 = reinterpret_cast<InternalNode*>(CHILDREN(node)[child_index +1]);
        assert(n1->N + n2->N + 1 <= inode_b);

        // move the pivot into n1
        KEYS(n1)[n1->N -1] = KEYS(node)[child_index];
        CHILDREN(n1)[n1->N] = CHILDREN(n2)[0];

        // move all elements from n2 to n1 (except the first pointer from n2)
        assert(n2->N > 0);
        memcpy(KEYS(n1) + n1->N, KEYS(n2), (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(CHILDREN(n1) + n1->N +1, CHILDREN(n2) +1, (n2->N -1) * sizeof(CHILDREN(n2)[0]));

        // update the sizes of the two nodes
        n1->N += n2->N;
        n2->N = 0;

        // deallocate the intermediate node
        delete_node(n2, child_depth); n2 = nullptr;
    }

    // finally, remove the pivot from the parent (current node)
    assert(node->N >= lowerbound(child_depth -1) || node == m_root);

    // node->N might become |a-1|, this is still okay in a remove operation as we are
    // going to rebalance this node in post-order
    K* keys = KEYS(node);
    Node** children = CHILDREN(node);
    for(size_t i = child_index +1, last = node->N -1; i < last; i++){
        keys[i -1] = keys[i];
        children[i] = children[i+1];
    }
    node->N--;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::rotate_right(InternalNode* node, size_t child_index, int child_depth, size_t need){
    bool child_is_leaf = child_depth >= m_height -1;

    assert(node != nullptr);
    assert(0 < child_index && child_index < node->N);
    assert(need > 0);
    assert(CHILDREN(node)[child_index-1]->N >= need);
    assert(CHILDREN(node)[child_index]->N + need <= (child_is_leaf ? leaf_b : inode_b));

    if(child_is_leaf){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index -1];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index];

        K* __restrict l1_keys = KEYS(l1);
        V* __restrict l1_values = VALUES(l1);
        K* __restrict l2_keys = KEYS(l2);
        V* __restrict l2_values = VALUES(l2);

        // shift elements in l2 by `need'
        for(size_t i = l2->N -1 + need; i > 0; i--){
            l2_keys[i] = l2_keys[i - need];
            l2_values[i] = l2_values[i - need];
        }

        // copy `need' elements from l1 to l2
        for(size_t i = 0; i < need; i++){
            l2_keys[i] = l1_keys[l1->N - need +i];
            l2_values[i] = l1_values[l1->N - need +i];
        }
        // update the split point
        KEYS(node)[child_index -1] = l2_keys[0];

        // update the cardinalities
        l2->N += need;
        l1->N -= need;
    } else { // the children are internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index -1];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index];

        K* __restrict n2_keys = KEYS(n2);
        Node** __restrict n2_children = CHILDREN(n2);
        K* __restrict n1_keys = KEYS(n1);
        Node** __restrict n1_children = CHILDREN(n1);

        // shift elements in n2 by `need'
        if(n2->N > 0){
            n2_children[n2->N + need -1] = n2_children[n2->N -1];
            for(size_t i = n2->N + need -2; i >= need; i--){
                n2_keys[i] = n2_keys[i - need];
                n2_children[i] = n2_children[i - need];
            }
        }
        // move the pivot from node to n2
        n2_keys[need -1] = KEYS(node)[child_index-1];
        n2_children[need -1] = CHILDREN(n1)[n1->N -1];

        // copy the remaining elements from n1 to n2
        size_t idx = n1->N - need;
        for(size_t i = 0; i < need -1; i--){
            n2_keys[i] = n1_keys[idx];
            n2_children[i] = n1_children[idx];
            idx++;
        }

        // update the pivot
        KEYS(node)[child_index-1] = KEYS(n1)[n1->N - need -1];

        n2->N += need;
        n1->N -= need;
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::rotate_left(InternalNode* node, size_t child_index, int child_depth, size_t need){
    bool child_is_leaf = child_depth >= m_height -1;

    assert(node != nullptr);
    assert(0 <= child_index && child_index < node->N);
    assert(CHILDREN(node)[child_index]->N + need <= (child_is_leaf ? leaf_b : inode_b));
    assert(CHILDREN(node)[child_index+1]->N >= need);

    if(child_is_leaf){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];

        K* __restrict l1_keys = KEYS(l1);
        V* __restrict l1_values = VALUES(l1);
        K* __restrict l2_keys = KEYS(l2);
        V* __restrict l2_values = VALUES(l2);

        // move `need' elements of l2 in l1
        for(size_t i = 0; i < need; i++){
            l1_keys[l1->N + i] = l2_keys[i];
            l1_values[l1->N + i] = l2_values[i];
        }

        // left shift elements by `need' in l2
        for(int64_t i = 0, sz = l2->N -need; i < sz; i++){
            l2_keys[i] = l2_keys[i+need];
            l2_values[i] = l2_values[i+need];
        }

        // update the pivot
        KEYS(node)[child_index] = l2_keys[0];

        // adjust the sizes
        l1->N += need;
        l2->N -= need;
    } else { // internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index +1];

        K* __restrict n1_keys = KEYS(n1);
        Node** __restrict n1_children = CHILDREN(n1);
        K* __restrict n2_keys = KEYS(n2);
        Node** __restrict n2_children = CHILDREN(n2);

        // add the pivot to n1
        assert(n1->N > 0);
        n1_keys[n1->N -1] = KEYS(node)[child_index];
        n1_children[n1->N] = n2_children[0];

        // move 'need -1' elements from n2 to n1
        size_t idx = n1->N;
        for(size_t i = 0; i < need -1; i++){
            n1_keys[idx] = n2_keys[i];
            n1_children[idx +1] = n2_children[i +1];
        }

        // update the pivot
        KEYS(node)[child_index] = n2_keys[need -1];

        // left shift elements by `need' in n2
        for(size_t i = 0, sz = n2->N -need -1; i < sz; i++){
            n2_keys[i] = n2_keys[i+need];
            n2_children[i] = n2_children[i+need];
        }
        n2_children[n2->N -need -1] = n2_children[n2->N -1];

        // adjust the sizes
        n1->N += need;
        n2->N -= need;
    }

}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::rebalance_lb(InternalNode* node, size_t child_index, int child_depth){
    assert(node != nullptr);
    assert(node->N > 1 || node == m_root);
    assert(child_index < node->N);

    // the child already contains more than a elements => nop
    size_t child_sz = CHILDREN(node)[child_index]->N;
    const size_t lb = lowerbound(child_depth);
    if(child_sz >= lb){ return; } // nothing to do!

    // okay, if this is the root && it has only one child, there is not much we can do
    if(node == m_root && node->N <= 1) return;

    // how many nodes do we need?
    int64_t need = lb - child_sz;

    // check if we can steal `need' nodes from its sibling
    bool can_rotate_right = false;
    if(child_index > 0){ // steal from left
        Node* child_left = CHILDREN(node)[child_index -1];
        if(child_left->N >= lb + need +1){
            rotate_right(node, child_index, child_depth, need +1);
            return; // done
        } else {
            can_rotate_right = child_left->N >= lb + need;
        }
    }

    bool can_rotate_left = false;
    if(child_index < node->N -1){ // steal from right
        Node* child_right = CHILDREN(node)[child_index +1];
        if(child_right->N >= lb + need +1){
            rotate_left(node, child_index, child_depth, need +1);
            return; // done
        } else {
            can_rotate_left = child_right->N >= lb + need;
        }

    }
    // we cannot steal `need +1' nodes, but maybe we can rotate just `need' nodes
    // bringing the size of child to |a|
    if(can_rotate_right){
        rotate_right(node, child_index, child_depth, need);
        return;
    }
    if(can_rotate_left){
        rotate_left(node, child_index, child_depth, need);
        return;
    }

    // both siblings contain |a -1 + a| elements, merge the nodes
    if(child_index < node->N -1){
        merge(node, child_index, child_depth);
    } else {
        assert(child_index > 0);
        merge(node, child_index -1, child_depth);
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::rebalance_rec(Node* node, const K range_min, const K range_max, int depth){
    bool is_leaf = depth >= m_height -1;

    // base case
    if(is_leaf){
        assert(node->N >= leaf_b/2 || node == m_root);
        return;
    }

    // rebalance the internal nodes
    InternalNode* inode = reinterpret_cast<InternalNode*>(node);
    K* __restrict keys = KEYS(inode);
    Node** children = CHILDREN(inode);
    size_t i = 0, inode_num_keys = inode->N -1;
    while(i < inode_num_keys && keys[i] < range_min) i++;

    rebalance_lb(inode, i, depth +1); // the first call ensures inode[i] >= |a+1| if possible, otherwise inode[i] >= |a|

    // if this is the root, check whether we need to reduce the tree if it has only one child
    if(node == m_root){
        bool reduced = reduce_tree(); // reduced is the same as checking root != node
        if(reduced){ return rebalance_rec(m_root, range_min, range_max, 0); }
    }

    rebalance_rec(children[i], range_min, range_max, depth +1);

    rebalance_lb(inode, i, depth +1); // the second time, it brings inode[i] from |a-1| to at least |a|

    // if this is the root, check whether we need to reduce the tree if it has only one child
    if(node == m_root && reduce_tree()){ return rebalance_rec(m_root, range_min, range_max, 0); }

    if(inode->N > 1 && i < inode->N -2 && keys[i] < range_max){
        rebalance_lb(inode, i +1, depth +1);
        if(node == m_root && reduce_tree()){ return rebalance_rec(m_root, range_min, range_max, 0); }
        rebalance_rec(children[i+1], range_min, range_max, depth +1);
        rebalance_lb(inode, i +1, depth +1); // ensure inode[i+1] >= |a|
        if(node == m_root && reduce_tree()){ return rebalance_rec(m_root, range_min, range_max, 0); }
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::reduce_tree(){
    bool result = false;

    while(m_height > 1 && m_root->N == 1){
        InternalNode* inode = reinterpret_cast<InternalNode*>(m_root);
        m_root = CHILDREN(inode)[0];
        inode->N = 0;
        delete_node(inode, 0);
        m_height--;

        result = true;
    }

    return result;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::remove_subtrees_rec0(Node* node, int depth){
    if(node == nullptr) return;
    bool is_leaf = depth >= m_height -1;

    if(!is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);
        for(size_t i = 0; i < inode->N; i++){
            remove_subtrees_rec0(children[i], depth +1);
            delete_node(children[i], depth +1);
            children[i] = nullptr;
        }
    } else {
        assert(m_cardinality >= node->N);
        m_cardinality -= node->N;
    }

    node->N = 0;
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::remove_subtrees(InternalNode* node, size_t index, size_t length, int children_depth){
    assert(node != nullptr);
    assert(index + length <= node->N);

    K* keys = KEYS(node);
    Node** children = CHILDREN(node);

    for(size_t i = index, last = index + length; i < last; i++){
        remove_subtrees_rec0(children[i], children_depth);
        delete_node(children[i], children_depth);
        children[i] = nullptr;
    }

    // if the length == node->N, then we are removing all elements
    assert(length < node->N || (index == 0 && node->N == length));
    if(length < node->N){
        // shift the pointers
        for(size_t i = index, last = node->N - length; i < last; i++){
            children[i] = children[i + length];
        }
        // shift the keys
        for(size_t i = (index > 0) ? index -1 : 0, last = node->N -1 - length; i < last; i++){
            keys[i] = keys[i + length];
        }
    }

    node->N -= length;
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::remove_keys(Node* node, const K range_min, const K range_max, int depth, bool* min_set, K* min_key){
    bool is_leaf = depth >= m_height -1;
    if(!is_leaf){
        bool retrebalance = false;
        bool tmp_min_set { false }; K tmp_min_key;
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t start = 0, N = inode->N;
        while(start < N -1 && KEYS(inode)[start] < range_min) start++;
        size_t end = start;
        while(end < N -1 && KEYS(inode)[end] <= range_max) end++;

        size_t remove_trees_start = start +1;
        size_t remove_trees_length = (end > start +1) ? end - start -1 : 0;

        // remove the keys at the head
        retrebalance |= remove_keys(CHILDREN(inode)[start], range_min, range_max, depth +1, min_set, min_key);
        if(CHILDREN(inode)[start]->empty()){
            remove_trees_start--;
            remove_trees_length++;
        }

        // remove the keys at the tail
        if(end > start){
            retrebalance |= remove_keys(CHILDREN(inode)[end], range_min, range_max, depth +1, &tmp_min_set, &tmp_min_key);
            if(tmp_min_set == false){ // empty block
                assert(CHILDREN(inode)[end]->empty());
                remove_trees_length++;
            } else {
                KEYS(inode)[end -1] = tmp_min_key;
            }
        }

        // remove whole trees
        if(remove_trees_length > 0){
            // before shifting the key containing the minimum for the next available block,
            // record into the variable *min
            if(min_set && start == 0){
                if(remove_trees_length < inode->N){
                    *min_set = true;
                    *min_key = KEYS(inode)[remove_trees_length -1];
                } else {
                    *min_set = false;
                }
            }

            remove_subtrees(inode, remove_trees_start, remove_trees_length, depth +1);
        }

        // when we remove from multiple children (i.e. start < end), we can have a situation like this:
        // inode->children: U | U | start = PR | FR | FR | FR | end = PR | U | U
        // where U = untouched, PR: subtree partially removed, FR: subtree wholly removed
        // then after remove_subtrees(...) is invoked, all FR branches are deleted from the array and the two PR
        // subtrees are next to each other. The next invocation ensures that only one of the two PRs has less |a| elements,
        // otherwise they are merged together.
        if(start < end && start+1 < inode->N && CHILDREN(inode)[start]->N + CHILDREN(inode)[start+1]->N <= 2* lowerbound(depth +1) -1){
            merge(inode, start, depth+1);
        }

        return retrebalance || inode->N < (inode_b/2);
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->N == 0) { // mmh
            if(min_set) *min_set = false;
            return true;
        }

        K* keys = KEYS(leaf);
        V* values = VALUES(leaf);

        if((keys[0] <= range_max) && (keys[leaf->N -1] >= range_min)) {
            size_t start = 0;
            while(keys[start] < range_min) start++;
            size_t end = start;
            while(end < leaf->N && keys[end] <= range_max) end++;
            size_t length = end - start;
            size_t last = start + leaf->N - end;
            for(size_t i = start; i < last; i++){
                keys[i] = keys[i + length];
                values[i] = values[i + length];
            }
            leaf->N -= length;
            assert(m_cardinality >= length);
            m_cardinality -= length;
        }

        if(min_set != nullptr){
            *min_set = leaf->N > 0;
            if(*min_set) *min_key = keys[0];
        }

        return leaf->N < (leaf_b/2);
    }

}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::remove(Node* node, const K keymin, const K keymax, int depth){
    // first pass, remove the elements
    bool rebalance = remove_keys(node, keymin, keymax, depth, nullptr, nullptr);

    if(rebalance){
        // quite an edge case, everything was removed from the tree, also the starting leaf, due to the subtree removal
        if(node == m_root && node->N == 0 && m_height > 1){
            std::cout << "this path" << std::endl;
            delete_node(m_root, 0);
            m_height = 1;
            m_root = create_leaf();
        } else {
            // standard case
            rebalance_rec(node, keymin, keymax, 0);
        }
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::remove(const K key){
    remove(m_root, key, key, 0); // that is, remove all keys in the range [key, key]
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::remove_any(Node* node, const K key, V* out_value, int depth, K* omin){
    assert(node != nullptr);

    while(depth < m_height -1){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = node->N;
        assert(N > 0);
        while(i < N -1 && KEYS(inode)[i] < key) i++;
        if(omin == nullptr && i < N -1 && KEYS(inode)[i] == key){
            // in case omin != nullptr, then i = 0 and we are already following the min
            // from another internal node. This tree has many duplicates.
            // The resulting omin has to be inode->keys[i] = inode->keys[0] and
            // all keys from block 0 are equal to inode->keys[0];
            // Nevertheless we keep percolating the tree to remove the key from the leaf.
            K newkey;
            bool result = remove_any(CHILDREN(inode)[i+1], key, out_value, depth +1, &newkey);
            KEYS(inode)[i] = newkey;
            rebalance_lb(inode, i+1, depth+1);
            return result; // stop the tail recursion
        } else if (CHILDREN(inode)[i]->N <= lowerbound(depth+1)){
            bool result = remove_any(CHILDREN(inode)[i], key, out_value, depth+1, omin); // it might bring inode->pointers[i]->N == |a-1|
            rebalance_lb(inode, i, depth +1);
            return result; // stop the tail recursion
        } else { // the node has already |a+1| children, no need to rebalance
            node = CHILDREN(inode)[i];
        }

        depth++;
    }

    { // base case, this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        size_t N = leaf->N;
        K* __restrict keys = KEYS(leaf);
        V* __restrict values = VALUES(leaf);
        bool element_removed {false};
        if(N > 0){
            if(keys[N-1] == key){
                if(out_value) *out_value = values[N-1];
                leaf->N -= 1;
                element_removed = true;
            } else if(keys[N-1] > key){
                size_t i = 0;
                while(i < N && keys[i] < key) i++;
                if(i < N && keys[i] == key){
                    if(out_value) *out_value = values[i];
                    for(size_t j = i; j < leaf->N -1; j++){
                        keys[j] = keys[j+1];
                        values[j] = values[j+1];
                    }
                    leaf->N -= 1;
                    element_removed = true;
                }
            }
        }

        if(omin && leaf->N > 0){
            *omin = keys[0];
        }

        return element_removed;
    }

}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::remove_any(const K key, V* out_value){
    bool element_removed = remove_any(m_root, key, out_value, 0, nullptr);

    if(element_removed){
        m_cardinality--;

        // shorten the tree
        if(m_root->N == 1 && m_height > 1){ // => the root is not a leaf
            InternalNode* iroot = reinterpret_cast<InternalNode*>(m_root);
            m_root = CHILDREN(iroot)[0];

            iroot->N = 0;
            delete_node(iroot, 0);
            m_height--;
        }
    }

    return element_removed;
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::find_any(const K key, V* out_value) const {
    Node* node = m_root; // start from the root
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N -1;
        assert(N > 0 && N <= inode_b);
        K* __restrict keys = KEYS(inode);
        while(i < N && keys[i] <= key) i++;
        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    size_t i = 0, N = leaf->N;
    K* __restrict keys = KEYS(leaf);
    while(i < N && keys[i] < key) i++;
    if(i < N && keys[i] == key){
        if(out_value) *out_value = VALUES(leaf)[i];
        return true;
    } else {
        return false;
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::find_first(const K key, K* out_key, V* out_value) const {
    if(out_value == nullptr){ throw std::invalid_argument("Output value is null"); }

    Node* node = m_root; // start from the root
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N -1;
        assert(N > 0 && N <= inode_b);
        K* __restrict keys = KEYS(inode);
        while(i < N && keys[i] < key) i++;
        node = CHILDREN(inode)[i];
    }

    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    size_t i = 0, N = leaf->N;
    K* __restrict keys = KEYS(leaf);
    while(i < N && keys[i] < key) i++;
    if(i < N && keys[i] == key){ // exact match
        if(out_key) *out_key = KEYS(leaf)[i];
        *out_value = VALUES(leaf)[i];
        return true;
    } else if (i == 0){ // the searched key is smaller than all keys stored in the whole tree
        assert(leaf->previous == nullptr);
        return false; // no hope, sorry
    } else if (i == N && leaf->next != nullptr && KEYS(leaf->next)[0] == key){ // check the successor (in case of duplicates, the traversal may end up a leaf before the actual node)
        if(out_key) *out_key = KEYS(leaf->next)[0];
        *out_value = VALUES(leaf->next)[0];
        return true;
    } else { // return the predecessor
        assert(i > 0);
        if(out_key) *out_key = KEYS(leaf)[i -1];
        *out_value =  VALUES(leaf)[i -1];
        return true;
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
bool DynamicIndex<K, V, inode_b, leaf_b>::find_last(const K key, K* out_key, V* out_value) const {
    if(out_value == nullptr){ throw std::invalid_argument("Output value is null"); }
    if(empty()) return false;

    Node* node = m_root; // start from the root
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    for(int depth = 0, l = m_height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        assert(inode->N >= 2);
        int64_t i = inode->N -2;
        K* __restrict keys = KEYS(inode);
        if(keys[i] > key){
            do { i--; } while(i >= 0 && keys[i] > key);
            if(i > 0 && keys[i -1] == key) i--; // exact match
        }
        node = CHILDREN(inode)[i +1];
    }

    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0);
    int64_t i = leaf->N -1;
    K* __restrict keys = KEYS(leaf);
    while(i >= 0 && keys[i] > key) i--;
    if(i < 0){ // `key' is smaller than all keys in the tree
        assert(leaf->previous == nullptr);
        return false;
    } else if ( i > 0 && keys[i -1] == key ){ // exact match
        if(out_key) *out_key = keys[i -1];
        *out_value = VALUES(leaf)[i -1];
        return true;
    } else { // successor
        if(out_key) *out_key = keys[i];
        *out_value = VALUES(leaf)[i];
        return true;
    }
}


template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::dump_data(std::ostream& out, Node* node, int depth) const {
    using namespace std;
    assert(node != nullptr);
    assert(depth < m_height);

    const bool is_leaf = depth == m_height -1;

    // preamble
    auto flags = out.flags();
    if(depth > 0) out << ' ';
    out << setw(depth * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << (is_leaf ? "L" : "I") << ' ';
    out << hex << node << dec << " N: " << node->N << '\n' ;
    out.setf(flags);

    // tabs
    auto dump_tabs = [&out, depth](){
        auto flags = out.flags();
        out << setw(depth * 2 + 5) << setfill(' ') << ' ';
        out.setf(flags);
    };

    if (!is_leaf){ // internal node
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        auto flags = out.flags();
        dump_tabs();
        out << "Keys: ";
        if(inode->N > 0){
            for(size_t i = 0; i < inode->N -1; i++){
                if(i > 0) out << ", ";
                out << i << ": " << KEYS(inode)[i];
            }
        }
        out << '\n';
        dump_tabs();
        out << "Ptrs: " << hex;
        for(size_t i = 0; i < inode->N; i++){
            if(i > 0) out << ", ";
            out << i << ": " << CHILDREN(inode)[i];
        }
        out << dec << endl;
        out.setf(flags);

        // recurse
        for(int i = 0, sz = inode->N; i < sz; i++){
            dump_data(out, CHILDREN(inode)[i], depth+1);
        }
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->empty()) return;

        auto flags = out.flags();

        dump_tabs();
        for(size_t i = 0; i < leaf->N; i++){
            if(i > 0) out << ", ";
            out << "<" << KEYS(leaf)[i] << ", " << VALUES(leaf)[i] << ">";
        }
        out << "\n";

        dump_tabs();
        out << "Prev: " <<  leaf->previous << ", Next: " << leaf->next;

        out << endl;

        out.setf(flags);
    }
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::dump_memory(std::ostream& out) const {
    out << "[DynamicIndex] cardinality: " << m_cardinality << ", height: " << m_height;

    auto mem_inodes = m_num_inodes * memsize_inode();
    out << ", inodes: " << m_num_inodes << " (" << to_string_with_unit_suffix(mem_inodes) << ") with node size: " << inode_b << " (" << to_string_with_unit_suffix(inode_b) << ")";
    auto mem_leaves = m_num_leaves * memsize_leaf();
    out << ", leaves: " << m_num_leaves << " (" << to_string_with_unit_suffix(mem_leaves) << ") with node size: " << leaf_b << " (" << to_string_with_unit_suffix(leaf_b) << ")";
    out << ", total: " << (m_num_inodes + m_num_leaves) << " (" << to_string_with_unit_suffix(mem_inodes + mem_leaves) << ")";
    out << std::endl;

#if defined(HAVE_LIBNUMA)
    if(numa_available() != -1){
        auto current_cpu = sched_getcpu();
        auto current_node = numa_node_of_cpu(current_cpu);
        auto tot_nodes = numa_num_configured_nodes();

        out << "--> Executing on cpu: " << current_cpu << ", node: " << current_node << ", total nodes: " << tot_nodes << "\n";

        if(tot_nodes > 1){
            // map the allocations of nodes
            std::vector<int> map_inodes(tot_nodes);
            std::vector<int> map_leaves(tot_nodes);

            std::function<void(Node*, int)> explore;
            explore = [this, &map_inodes, &map_leaves, &explore](Node* node, int level){
                bool is_leaf = level == this->m_height;

                // dfs on the children
                if(!is_leaf){
                    auto inode = reinterpret_cast<InternalNode*>(node);
                    auto children = CHILDREN(inode);
                    for(size_t i = 0; i < inode->N; i++){
                        explore(children[i], level +1);
                    }
                }

                int numa_node {-1};
                auto rc = get_mempolicy(&numa_node, nullptr, /* ignored*/ 0, node, MPOL_F_NODE | MPOL_F_ADDR);
                if( rc != 0 ){
                    std::cerr << "[ABTree::dump_memory] get_mempolicy: " << strerror(errno) << " (" << errno << "), logical node: " << node << std::endl;
                } else if( numa_node < 0 || numa_node >= (int) map_inodes.size() ){
                    std::cerr << "[ABTree::dump_memory] get_mempolicy, invalid result: " << numa_node << ", logical node: " << node << std::endl;
                } else if(is_leaf) {
                    map_leaves[numa_node]++;
                } else {
                    map_inodes[numa_node]++;
                }
            };

            explore(m_root, 1);

            for(size_t i = 0; i < map_inodes.size(); i++){
                auto mem_inodes = map_inodes[i] * memsize_inode();
                auto mem_leaves = map_leaves[i] * memsize_leaf();
                out << "--> Node: " << i;
                out << ", inodes: " << map_inodes[i] << " (" << to_string_with_unit_suffix(mem_inodes) << ")";
                out << ", leaves: " << map_leaves[i] << " (" << to_string_with_unit_suffix(mem_leaves) << ")";
                out << ", total: " << (map_inodes[i] + map_leaves[i]) << " (" << to_string_with_unit_suffix(mem_inodes + mem_leaves) << ")";
                out << "\n";
            }

        } // tot_nodes > 1
    }
#endif
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::dump(std::ostream& out) const {
    dump_memory(out);
    dump_data(out, m_root, 0);
}

template<typename K, typename V, int inode_b, int leaf_b>
void DynamicIndex<K, V, inode_b, leaf_b>::dump() const {
    dump(std::cout);
}

} // namespace pma

#endif /* GENERIC_DYNAMIC_INDEX_HPP_ */
