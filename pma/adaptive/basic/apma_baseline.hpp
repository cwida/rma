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

/*
 * This an indexed PMA, with a dynamic B+ tree, and the adaptive strategy of BH07.
 * The PMA part is based on btree_pma_v2:
 * - Dynamic binary index, based on an (a,b)-tree
 * - Explicit segments, but the cardinalities for the segments are not recorded
 * - Interleaved keys & values, that is, elements are pairs <key, value>
 * The adaptive strategy is based on apma_hu:
 * - Predictor based on a priority queue
 * - Indexed elements on the predictor are the segments
 */

#ifndef ADAPTIVE_BASIC_APMA_BASELINE_HPP_
#define ADAPTIVE_BASIC_APMA_BASELINE_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

#include <cinttypes>
#include <ostream>
#include <vector>

namespace pma {

namespace apma_baseline_details {
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
    const bool is_leaf : 8; // is this an InnerNode or a Leaf?
    size_t N : 56; // the number of elements contained

    bool empty() const;
};

// An internal node of the B-Tree
struct InternalNode : public Node {
//    int64_t* keys; // capacity |B|
//    Node** pointers; // capacity |B+1|, pointers to the children nodes (inodes or leaves)
};

struct Leaf : public Node {
//    int64_t* keys; // capacity |B|
//    int64_t* values; // capacity |B+1|, these are pointers (indices) to the storage (PMA)
//    int64_t* cardinalities; // number of elements indexed by values[i] in the storage (PMA)
    // Leaves have pointers to the next/previous leaves, forming a linked list
    Leaf* prev;
    Leaf* next;
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

class Iterator : public ::pma::Iterator {
    const apma_baseline_details::PMA& storage;
    size_t pos;
    int64_t key_max;

public:
    Iterator(const apma_baseline_details::PMA& storage, size_t start, int64_t key_min, int64_t key_max);

    virtual bool hasNext() const;
    virtual std::pair<int64_t, int64_t> next();
};

/*****************************************************************************
  *                                                                           *
  *   Predictor                                                               *
  *                                                                           *
  *****************************************************************************/
 /**
  * An element in the predictor
  */
 struct Item {
     uint64_t m_pointer : 48; // pointer to the key in APMA
     uint64_t m_count : 16; // count associated to the key
 };

 std::ostream& operator <<(std::ostream&, const Item&);

 class Predictor {
     Item* m_buffer; // underlying buffer
     size_t m_capacity; // capacity of the circular array
     size_t m_tail; // pointer to the tail of the array
     size_t m_head; // pointer to the head of the array
     size_t m_count_max; // the max value a key in the array can hold
     bool m_empty; // is the circular buffer empty?

     /**
      * Allocate a buffer with the given capacity;
      */
     static Item* allocate_buffer(size_t capacity);

     /**
      * Deallocate the existing buffer
      */
     static void deallocate_buffer(Item*& buffer);

     /**
      * Find the position of the key in the circular array, or return -1 if not present
      */
     int64_t get_position(int64_t pointer) const;

     /**
      * Move the given item in the queue ahead of one position, if it's not already in the front
      * Return the new position of the item: either item_position +1, or 0 if it the item was at
      * the end of the circular array.
      */
     size_t move_ahead(size_t item_position);

     /**
      * Decrease the value for the element at the tail of the queue. If its count reaches 0, remove
      * the element from the queue, effectively moving the tail of one position ahead.
      */
     void decrease_tail();

     /**
      * Insert the key at the front of the queue, pointing to the cell in `pma_position' in the PMA.
      * As side effect, move the front of the queue ahead of one position.
      */
     void insert_new_element(uint64_t pointer);

 public:
     /**
      * Create a new instance with the given size
      */
     Predictor(size_t size, size_t max_count);

     /**
      * Destructor
      */
     ~Predictor();

     /**
      * Set the max value a key in the predictor can have
      */
     void set_max_count(size_t value);

     /**
      * Insert/Update the count for the given key in the predictor
      */
     void update(uint64_t pointer);

     /**
      * Remove all entries in the container and resize it with a new capacity
      */
     void resize(size_t capacity);

     /**
      * Gets all elements in the predictor in the range [min, max]. The resulting
      * list is sorted by the key
      */
     std::vector<Item> weighted_elements(uint64_t min, uint64_t max);

     /**
      * Alias for the method #weighted_elements
      */
     std::vector<Item> items(uint64_t min, uint64_t max);

     /**
      * Reset the pointer for the element at the given position
      */
     void reset_ptr(size_t index, size_t pma_position);

     /**
      * Check whether the container is empty
      */
     bool empty() const;

     /**
      * Check whether the container is full
      */
     bool full() const;

     /**
      * Retrieve the number of elements stored
      */
     size_t size() const;

     /**
      * Dump the content of the data structure, for debug purposes.
      */
     void dump(std::ostream& out) const;
     void dump() const;
 };

} // namespace apma_baseline_details

class APMA_Baseline : public InterfaceRQ {
private:
    apma_baseline_details::BTree index;
    apma_baseline_details::PMA storage;
    apma_baseline_details::Predictor predictor;
    const double predictor_scale; // beta parameter to adjust the resizing of the predictor

    static constexpr int storage_min_capacity = 8; // the minimum capacity of the underlying PMA
    static constexpr double r_0 = 0.5; // highest threshold for the lower bound
    static constexpr double t_0 = 0.75; // lowest threshold for the upper bound

    // Initialize the index
    apma_baseline_details::BTree index_initialize(size_t index_B, size_t storage_B);
    apma_baseline_details::BTree index_initialize(size_t intnode_a, size_t intnode_b, size_t leaf_a, size_t leaf_b, size_t storage_a, size_t storage_b);

    // Initialize the storage
    void storage_initialize(size_t capacity);

    // Create a new leaf
    apma_baseline_details::Leaf* create_leaf() const;

    // Create a new internal node
    apma_baseline_details::InternalNode* create_internal_node() const;

    // Remove a node
    void delete_node(apma_baseline_details::Node* node);
    void delete_node(apma_baseline_details::InternalNode* node);
    void delete_node(apma_baseline_details::Leaf* node);

    // Retrieve the keys of an internal node, capacity |B|
    int64_t* keys(const apma_baseline_details::InternalNode* inode) const;

    // Retrieve the children of an internal node (inodes or leaves), capacity |B+1|
    apma_baseline_details::Node** children(const apma_baseline_details::InternalNode* inode) const;

    // The keys in a leaf, capacity |B|
    int64_t* keys(const apma_baseline_details::Leaf* leaf) const;

    // Pointers (indices) to the storage PMA, capacity |B+1|
    uint64_t* values(const apma_baseline_details::Leaf* leaf) const;

    // Number of elements indexed by values[i] in the storage PMA
    uint16_t* cardinalities(const apma_baseline_details::Leaf* leaf) const;

    // It increases the height of tree by 1, by splitting the current root in half and
    // introducing a new root.
    void index_split_root();

    // It splits the child of `node' at index `child' in half and adds the new node as
    // a new child of `node'.
    void index_split_node(apma_baseline_details::InternalNode* node, size_t child);

    // Add a new element in the leaf
    void index_leaf_augment(apma_baseline_details::Leaf*, size_t position);

    // Insert the given key/value in the subtree rooted at the given `node'
    void insert(apma_baseline_details::Node* node, int64_t key, int64_t value);

    // Propagate the insertion to the leaf
    void insert_leaf(apma_baseline_details::Leaf* node, int64_t key, int64_t value);

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    void insert_storage(apma_baseline_details::Leaf* leaf, size_t index_leaf, int64_t key, int64_t value);

    // Move the elements in the storage forward by 1 position
    void storage_shift_right(apma_baseline_details::Leaf* leaf, size_t index_leaf, size_t pos, size_t length);

    /**
     * Compute the lower (out_a) and upper (out_b) threshold for the segments at the given `height'
     */
    void storage_thresholds(std::size_t height, double& out_a, double& out_b) const;

    /**
     * Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
     */
    void storage_rebalance(apma_baseline_details::Leaf* leaf, size_t index_leaf, size_t index_insert);

    /**
     * Resize the storage & update the pointers in the index
     */
    void storage_resize();

    /**
     * Spread unequally the elements, according to the cardinalities set in the `partitions' vector.
     */
    void storage_spread(const std::vector<uint16_t>& partitions, apma_baseline_details::Leaf* leaf, size_t index_leaf, size_t num_elements, size_t window_start, size_t window_length);

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

    void dump_node(std::ostream& out, apma_baseline_details::Node* node, size_t depth, bool* integrity_check) const;
    void dump_storage(std::ostream& out) const;
    void dump_predictor(std::ostream& out) const;

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

    /**
     * Retrieve the number of elements to allocate in each partition, according to the current state of the
     * predictor and the `adaptive' strategy
     */
    decltype(auto) apma_partitions(int height, size_t cardinality, size_t segment_start, size_t num_segments);

    /**
     * Recursive step, compute the left & right cardinality for the given window
     */
    void apma_compute_rec(std::vector<uint16_t>& partitions, int64_t partitions_base, int height, size_t cardinality, size_t segment_start, size_t num_segments, const std::vector<apma_baseline_details::Item>& weights, size_t weights_start, size_t weights_length);

public:
    APMA_Baseline();

    APMA_Baseline(size_t index_B, size_t storage_B, double predictor_scale = 1.0);

    APMA_Baseline(size_t index_A, size_t index_B, size_t storage_A, size_t storage_B, double predictor_scale = 1.0);

    virtual ~APMA_Baseline();

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

    virtual pma::Interface::SumResult sum(int64_t, int64_t max) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<pma::Iterator> iterator() const override;

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



#endif /* ADAPTIVE_BASIC_APMA_BASELINE_HPP_ */
