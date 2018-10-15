/*
 * static_abtree.hpp
 *
 *  Created on: Jan 16, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef BTREE_STATIC_ABTREE_HPP_
#define BTREE_STATIC_ABTREE_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "pma/generic/static_index.hpp"

#include <utility>
#include <vector>

namespace abtree {

class StaticABTree; // forward decl.

namespace static_abtree_detail {

/**
 * Internal details on the layout of each level in the static ab-tree
 */
struct LevelInfo {
    size_t m_offset; // the starting offset for the given level in the array m_tree
    size_t m_elements_per_node; // the size of each node in this level
    size_t m_num_odd_nodes; // nodes in [0, m_num_odd_nodes) have an additional child
};

/**
 * Java-style iterator for StaticABTree
 */
class SAB_Iterator : public pma::Iterator {
    const StaticABTree* m_instance; // attached instance
    size_t m_offset; // current position
    size_t m_end; // final position

public:
    /**
     * Perform a scan in [begin_incl, end_excl)
     */
    SAB_Iterator(const StaticABTree* instance, size_t begin_incl, size_t end_excl);

    /**
     * Check whether a next element exists
     */
    bool hasNext() const override;

    /**
     * Return the next element
     */
    std::pair<int64_t, int64_t> next() override;
};

} // namespace static_abtree_detail

class StaticABTree : public pma::InterfaceRQ {
private:
    friend class static_abtree_detail::SAB_Iterator;
    StaticABTree(const StaticABTree&) = delete;
    StaticABTree& operator=(const StaticABTree&) = delete;

    pma::StaticIndex m_index; // the index to the elements array
    int64_t* m_keys; // a dense static array containing the ordered sequence of keys
    int64_t* m_values; // a dense static array containing the ordered sequence of values
    size_t m_cardinality; // the number of elements contained in the arrays m_keys/m_values
    using delta_t = std::vector<std::pair<int64_t, int64_t>>;
    delta_t m_delta;
    const size_t m_leaf_size; // the number of

    // Return the first position such that the related key is greater or equal than given search_key. The scan
    // starts from the parameter `start' and proceeds forwards.
    size_t scan_array_forwards0(size_t start, int64_t search_key) const;

    // Return the first position such that the related key is less or equal than given search_key. The scan
    // starts from the parameter `start' and proceeds backwards.
    size_t scan_array_backwards0(size_t start, int64_t search_key) const;

    // Find the minimum index i_min in the array such that min <= keys[i_min] and
    // the minimum index i_max such that max < keys[i_max]
    std::pair<size_t, size_t> find_minmax_indices(int64_t min, int64_t max) const;

    // Dump helpers
    void dump0_array() const;
    void dump0_delta() const;

    // Actual routine to rebuild the dense arrays and the tree
    void build0();
public:
    /**
     * Create an empty AB-Tree with a default page size
     */
    StaticABTree();

    /**
     * Create an AB-Tree with a given inner node and leaf size
     */
    StaticABTree(size_t inode_size, size_t leaf_size);

    /**
     * Default destructor
     */
    ~StaticABTree();

    /**
     * Append the given <key, value> in the delta array
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Rebuild the tree merging the current elements with the elements in the delta
     */
    void build() override;

    /**
     * Return the value associated to the element with the given `key', or -1 if not present.
     * In case of duplicates, it returns the value of one of the qualifying elements.
     */
    int64_t find(int64_t key) const override;

    /**
     * Return the number of elements in the static tree, without taking into account the delta
     */
    std::size_t size() const override;

    /**
     * Check whether the data structure is empty, withouth taking into account the delta
     */
    bool empty() const;

    /**
     * Find all elements in the interval [min, max]
     */
    std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

    /**
     * Scan all elements in the container
     */
    std::unique_ptr<pma::Iterator> iterator() const override;

    /**
     * Sum all elements in the range [min, max]
     */
    SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Report the memory footprint (in bytes) of the whole data structure
     */
    size_t memory_footprint() const override;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    void dump() const override;
};

} // namespace abtree


#endif /* BTREE_STATIC_ABTREE_HPP_ */
