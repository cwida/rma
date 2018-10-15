/*
 * BTreePMA_v2.hpp
 *
 *  Created on: Jun 27, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef BTREE_PMA_V2
#define BTREE_PMA_V2

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

#include <cinttypes>
#include <cmath>
#include <iostream>
#include <memory> // unique_ptr
#include <vector>


namespace pma {

namespace btree_pma_v2_detail {

/*****************************************************************************
 *                                                                           *
 *   PMA elements (key/value pairs)                                          *
 *                                                                           *
 *****************************************************************************/
template<typename K, typename V>
struct PMA_Element {
    K key;
    V value;

    PMA_Element(): key(), value() { }
    PMA_Element(K& key, V& value) : key(key), value(value) { }
};

template<typename K, typename V>
std::ostream& operator<<(std::ostream& out, const PMA_Element<K, V>& element){
    out << "<" << element.key << ", " << element.value << ">";
    return out;
}

using element_t = PMA_Element<int64_t, int64_t>;

/*****************************************************************************
 *                                                                           *
 *   Internal PMA                                                            *
 *                                                                           *
 *****************************************************************************/

struct PMA {
    element_t* m_elements; // the elements contained
    size_t m_capacity; // the size of the array elements
    size_t m_segment_capacity; // the capacity of a single segment
    size_t m_height; // the height of the binary tree for elements
    size_t m_cardinality; // the number of elements contained
};


/*****************************************************************************
 *                                                                           *
 *   Internal B+ Tree                                                        *
 *                                                                           *
 *****************************************************************************/

// forward declarations
struct Node;
struct InternalNode;
struct Leaf;
struct BTree;

// A Node in the B+ Tree can be an internal node (InnerNode) or a Leaf (Leaf)
struct Node{
//    InternalNode* parent; // a parent can only be an InnerNode
    const bool is_leaf : 8; // is this an InnerNode or a Leaf?
    size_t N : 56; // the number of elements contained

//    Node(InternalNode* parent, bool leaf): parent(parent), is_leaf(leaf), N(0) { };
    bool empty() const;
//    virtual ~Node();
};

// An internal node of the B-Tree
struct InternalNode : public Node {
//    int64_t* keys; // capacity |B|
//    Node** pointers; // capacity |B+1|, pointers to the children nodes (inodes or leaves)
//
//    InternalNode(InternalNode* parent, size_t capacity);
//    ~InternalNode();
};

struct Leaf : public Node {
//    int64_t* keys; // capacity |B|
//    int64_t* values; // capacity |B+1|, these are pointers (indices) to the storage (PMA)
//    int64_t* cardinalities; // number of elements indexed by values[i] in the storage (PMA)
    // Leaves are linked as linked list
    Leaf* prev;
    Leaf* next;

//    Leaf(InternalNode* parent, size_t capacity);
//    ~Leaf();
};

struct BTree {
    // The lower & upper bounds of the B-Tree node, inclusive
    const size_t intnode_a; // lower bound for internal nodes
    const size_t intnode_b; // upper bound for internal nodes
    const size_t leaf_a; // lower bound for leaves
    const size_t leaf_b; // upper bound for leaves
    const size_t storage_a; // lower bound for the number of elements in the PMA
    const size_t storage_b; // upper bound for the number of elements in the PMA
    Node* root; // current root
};

inline bool Node::empty() const {
    return N == 0;
}


/*****************************************************************************
 *                                                                           *
 *   Internal iterator                                                       *
 *                                                                           *
 *****************************************************************************/

class IteratorImpl : public pma::Iterator {
    const btree_pma_v2_detail::PMA& storage;
    size_t pos;
    int64_t key_max;

public:
    IteratorImpl(const btree_pma_v2_detail::PMA& storage, size_t start, int64_t key_min, int64_t key_max);

    virtual bool hasNext() const;
    virtual std::pair<int64_t, int64_t> next();
};

} // namespace btree_pma_v2_detail



class BTreePMA_v2 : public InterfaceRQ {
private:
    btree_pma_v2_detail::BTree index;
    btree_pma_v2_detail::PMA storage;

    static constexpr int storage_min_capacity = 8; // the minimum capacity of the underlying PMA
    static constexpr double r_0 = 0.5; // highest threshold for the lower bound
    static constexpr double t_0 = 0.75; // lowest threshold for the upper bound

    // Initialize the index
    btree_pma_v2_detail::BTree index_initialize(size_t index_B, size_t storage_B);
    btree_pma_v2_detail::BTree index_initialize(size_t intnode_a, size_t intnode_b, size_t leaf_a, size_t leaf_b, size_t storage_a, size_t storage_b);

    // Initialize the storage
    void storage_initialize(size_t capacity);

    // Create a new leaf
    btree_pma_v2_detail::Leaf* create_leaf() const;

    // Create a new internal node
    btree_pma_v2_detail::InternalNode* create_internal_node() const;

    // Remove a node
    void delete_node(btree_pma_v2_detail::Node* node);
    void delete_node(btree_pma_v2_detail::InternalNode* node);
    void delete_node(btree_pma_v2_detail::Leaf* node);

    // Retrieve the keys of an internal node, capacity |B|
    int64_t* keys(const btree_pma_v2_detail::InternalNode* inode) const;

    // Retrieve the children of an internal node (inodes or leaves), capacity |B+1|
    btree_pma_v2_detail::Node** children(const btree_pma_v2_detail::InternalNode* inode) const;

    // The keys in a leaf, capacity |B|
    int64_t* keys(const btree_pma_v2_detail::Leaf* leaf) const;

    // Pointers (indices) to the storage PMA, capacity |B+1|
    uint64_t* values(const btree_pma_v2_detail::Leaf* leaf) const;

    // Number of elements indexed by values[i] in the storage PMA
    uint16_t* cardinalities(const btree_pma_v2_detail::Leaf* leaf) const;

    // It increases the height of tree by 1, by splitting the current root in half and
    // introducing a new root.
    void index_split_root();

    // It splits the child of `node' at index `child' in half and adds the new node as
    // a new child of `node'.
    void index_split_node(btree_pma_v2_detail::InternalNode* node, size_t child);

    // Add a new element in the leaf
    void index_leaf_augment(btree_pma_v2_detail::Leaf*, size_t position);

    // Insert the given key/value in the subtree rooted at the given `node'
    void insert(btree_pma_v2_detail::Node* node, int64_t key, int64_t value);

    // Propagate the insertion to the leaf
    void insert_leaf(btree_pma_v2_detail::Leaf* node, int64_t key, int64_t value);

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    void insert_storage(btree_pma_v2_detail::Leaf* leaf, size_t index_leaf, int64_t key, int64_t value);

    // Move the elements in the storage forward by 1 position
    void storage_shift_right(btree_pma_v2_detail::Leaf* leaf, size_t index_leaf, size_t pos, size_t length);

    /**
     * Get the lower and upper bounds of the segment containing the position `pos' at the given `height'
     * Output:
     * - out_capacity: the capacity of the segment
     * - out_lb: the lower bound for the segment (inclusive)
     * - out_ub: the upper bound for the segment (inclusive)
     */
//    void storage_bounds(size_t pos, std::size_t height, int64_t& out_capacity, int64_t& out_lb, int64_t& out_ub) const;

    /**
     * Compute the lower (out_a) and upper (out_b) threshold for the segments at the given `height'
     */
    void storage_thresholds(std::size_t height, double& out_a, double& out_b) const;

    /**
     * Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
     */
    void storage_rebalance(btree_pma_v2_detail::Leaf* leaf, size_t index_leaf, size_t index_insert);

    /**
     * Resize the storage & update the pointers in the index
     */
    void storage_resize();

    /**
     * Equally spread the elements in the interval [lb, ub]
     */
    void storage_spread(btree_pma_v2_detail::Leaf* leaf, size_t index_leaf, size_t num_elements, size_t window_start, size_t window_length);

    /**
     * Set the presence flag for the element at the given `position'. A `value' of true implies
     * that there exist an element at the given `position'. The method takes of resizing the
     * storage.bitset to account for the number of present elements
     */
    template<bool value>
    void storage_set_presence(size_t position);

    /**
     * Returns an empty iterator, i.e. with an empty record set!
     */
    std::unique_ptr<pma::Iterator> empty_iterator() const;

    void dump_node(std::ostream& out, btree_pma_v2_detail::Node* node, size_t depth, bool* integrity_check) const;
    void dump_storage(std::ostream& out) const;

    /**
     * Check whether the given position is empty
     */
    bool storage_is_cell_empty(size_t position) const;

    /**
     * Retrieve the number of segments composing the storage PMA
     */
    size_t storage_get_num_segments() const;

    /**
     * Check whether the given segment is full
     */
    bool storage_is_segment_full(size_t segment_id) const;

    /**
     * Retrieve the number of elements in the given segment
     */
    size_t storage_get_segment_cardinality(size_t segment_id) const;

public:
    BTreePMA_v2();

    BTreePMA_v2(size_t index_B, size_t storage_B);

    BTreePMA_v2(size_t index_A, size_t index_B, size_t storage_A, size_t storage_B);

    virtual ~BTreePMA_v2();

    /**
     * Insert the given key/value
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Find the element with the given `key'. It returns its value if found, otherwise the value -1.
     * In case of duplicates, which element is returned is unspecified.
     */
    virtual int64_t find(int64_t key) const override;

    virtual std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<pma::Iterator> iterator() const override;

    // Sum all keys & values in the range [min, max]
    virtual SumResult sum(int64_t min, int64_t max) const override;

    // The number of elements stored
    virtual size_t size() const override;

    // Is this container empty?
    bool empty() const;

    // Dump the content of the data structure to the given output stream (for debugging purposes)
    virtual void dump(std::ostream& out) const;

    // Dump the content of the data structure to stdout (for debugging purposes)
    virtual void dump() const override;
};

} // namespace pma

#endif /* BTREE_PMA_V1 */
