/*
 * art.hpp
 *
 *  Created on: 24 Apr 2018
 *      Author: Dean De Leo
 *
 * Wrapper for the Adaptive Radix Tree implementation by Florian Scheibner
 * https://github.com/flode/ARTSynchronized
 */

#ifndef ABTREE_ART_HPP_
#define ABTREE_ART_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "third-party/art/Tree.h"

namespace abtree {

class ART : public pma::InterfaceRQ {
    // Elements are ultimately stored in a linked list of blocks, named leaves
    struct Leaf {
        // remove the ctors
        Leaf() = delete;
        Leaf(const Leaf&) = delete;
        Leaf& operator=(const Leaf&) = delete;

        size_t N; // number of elements in the tree
        Leaf* next; // link to the next leaf in the linked list
        Leaf* previous; // link to the previous leaf in the linked list
    };
    int64_t* KEYS(const Leaf* leaf) const;
    int64_t* VALUES(const Leaf* leaf) const;

    // Iterator, for the range queries
    class Iterator : public pma::Iterator {
      friend class ART;
      const ART* tree;
      int64_t max;
      Leaf* block;
      size_t pos;

      Iterator(const ART* tree, int64_t max, Leaf* leaf, int64_t pos);

    public:
      virtual bool hasNext() const override;
      virtual std::pair<int64_t, int64_t> next() override;
    };

    // Translate a key from humans into a key for the ART tree
    struct LoadKeyImpl : public ART_unsynchronized::LoadKeyInterface {
        ART* m_art; // pointer to the ART_nr data structure

        LoadKeyImpl(ART* tree);
        void operator() (const ART_unsynchronized::TID tid, ART_unsynchronized::Key& key) override;
        void store(int64_t pivot, ART_unsynchronized::Key& key);
    };
    friend struct LoadKeyImpl;

    // Create a new leaf
    Leaf* create_leaf();

    // Get the memory size of an internal node / leaf
    size_t memsize_leaf() const;

    // Get the minimum [first] key stored in the given leaf
    int64_t get_pivot(void* leaf) const;

    // Insert the given key in the leaf. Return the current minimum, i.e. the first element,
    // in the leaf
    int64_t leaf_insert(Leaf* leaf, int64_t key, int64_t value);

    // Insert the given key/vaue in the index
    void index_insert(int64_t key, Leaf* value);
    void index_insert(std::pair<int64_t, Leaf*> pair);

    // Find the first element that matches the given key in the leaf. Return the position
    // of the element, or -1 if it's not found
    int64_t leaf_find(Leaf* leaf, int64_t key) const noexcept;

    // Find the leaf having the greatest pivot that is less or equal than the given key
    Leaf* index_find_leq(int64_t key) const;

    // Remove the given pair from the index
    void index_remove(int64_t key, Leaf* value);

    // Remove the element with the given `key' from the leaf, return its value, or -1 if not found
    int64_t leaf_remove(int64_t key, Leaf*& leaf, bool* out_update_min);

    // Rebalance a leaf, by merge or share with one of its siblings, when its cardinality is less than m_leaf_block_size/2
    void rebalance(Leaf* leaf);
    void share_left(Leaf* leaf, int64_t need);
    void share_right(Leaf* leaf, int64_t need);
    void merge(Leaf* l1, Leaf* l2);

    // Split the given leaf. Return the key & the ptr to the new leaf created.
    std::pair<int64_t, Leaf*> split(Leaf* leaf);

    // Create an iterator with the given parameters
    std::unique_ptr<ART::Iterator> create_iterator(int64_t max, Leaf* block, int64_t pos) const;

    // Create an iterator for the provided interval starting from the given leaf
    std::unique_ptr<ART::Iterator> leaf_iterator(Leaf* leaf, int64_t min, int64_t max) const;

    // Perform an aggregate sum starting from the given leaf
    SumResult leaf_sum(Leaf* leaf, int64_t i, int64_t max) const;

    // Dump helpers
    void dump_leaves() const;

    std::shared_ptr<LoadKeyImpl> m_load_key; // Store the search key for an element in the index
    ART_unsynchronized::Tree m_index; // the ART index
    Leaf* m_first; // the first leaf in the linked list of elements
    const uint64_t m_leaf_block_size;
    uint64_t m_cardinality; // total number of elements in the data structure
    uint64_t m_leaf_count; // total number of leaves currently present

    // Wrapper interface for the ART index
public:
    ART(uint64_t leaf_block_size);

    virtual ~ART();

    void insert(int64_t key, int64_t value) override;

    int64_t remove(int64_t key) override;

    int64_t find(int64_t key) const override;

    std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

    std::unique_ptr<pma::Iterator> iterator() const override;

    SumResult sum(int64_t min, int64_t max) const override;

    size_t size() const override;

    bool empty() const noexcept;

    void dump() const override;

    size_t memory_footprint() const override;
};

} // namespace abtree

#endif /* ABTREE_ART_HPP_ */
