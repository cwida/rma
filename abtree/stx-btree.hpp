/*
 * stx-btree.hpp
 *
 *  Created on: 26 Nov 2017
 *      Author: Dean De Leo
 *
 *
 *  This is actually a wrapper to the STX-Btree implementation by
 *  Timo Bingmann: https://github.com/bingmann/stx-btree/
 *
 */

#ifndef STX_BTREE_HPP_
#define STX_BTREE_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include <memory> // unique_ptr

// forward decl.
namespace abtree { class STXBtree; }
#define BTREE_FRIENDS friend class ::abtree::STXBtree;

#include "third-party/stx/btree.h"

namespace abtree {

/**
 * A wrapper with the PMA_Enhanced interface for the implementation by
 * T. Bingmann of B+ Trees.
 *
 * There are some issues though:
 * - The max size of a leaf/inner node can only be set at compile time,
 *   by defining the macros STX_BTREE_LEAF_B and STX_BTREE_INDEX_B
 * - There is no proper support for range queries when the minimum of
 *   the interval is not in the B+ tree.
 * - No duplicates allowed (the flag needs to be flipped at compile time)
 */
class STXBtree : public pma::InterfaceRQ {
public:
    /**
     * Compile-time properties for the BTree. Define STX_BTREE_LEAF_B & STX_BTREE_INDEX_B
     * at compile time
     * Derived from stx::btree_default_map_traits
     */
    struct btree_traits {
        // Sanity checks
        static const bool selfverify = false;

        // Debug mode
        static const bool debug = false;


#if !defined(STX_BTREE_LEAF_B)
        /// Number of slots in each leaf of the tree. Estimated so that each node
        /// has a size of about 256 bytes.
        static const int leafslots = stx::btree_default_map_traits<int64_t, int64_t>::leafslots;
#else
        static const int leafslots = STX_BTREE_LEAF_B;
#endif

#if !defined(STX_BTREE_INDEX_B)
        // Number of slots in each inner node of the tree. Estimated so that each node
        // has a size of about 256 bytes.
        static const int innerslots = stx::btree_default_map_traits<int64_t, int64_t>::innerslots;
#else
        static const int innerslots = STX_BTREE_INDEX_B;
#endif

        // Threshold when to switch to a binary search
        static const size_t binsearch_threshold = 256;
    };

private:
    using btree_t = stx::btree<int64_t, int64_t, std::pair<int64_t, int64_t>, std::less<int64_t>, btree_traits>;

    class IteratorImpl : public pma::Iterator {
        using iterator_t = btree_t::const_iterator;

        iterator_t iterator;
        iterator_t end;
        int64_t max;

    public:
        IteratorImpl(const iterator_t& iterator, const iterator_t& end, int64_t max);

        ~IteratorImpl() = default;

        bool hasNext() const override;

        std::pair<int64_t, int64_t> next() override;
    };

    btree_t impl;
    int64_t minimum;

public:
    STXBtree();

    virtual ~STXBtree();

    /**
     * Insert the given <key, value> in the container
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Return the value associated to the element with the given `key', or -1 if not present.
     * In case of duplicates, it returns the value of one of the qualifying elements.
     */
    int64_t find(int64_t key) const override;

    /**
     * Return the number of elements in the container
     */
    std::size_t size() const override;

    /**
     * Scan all elements in the container
     */
    virtual std::unique_ptr<pma::Iterator> iterator() const override;

    /**
     * Sum all elements in the range [min, max]
     */
    virtual SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    virtual void dump() const override;

    /**
     * Memory statistics
     */
    virtual void dump_memory_usage() const;

    /**
     * Find all elements in the interval [min, max]
     */
    virtual std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

};

} // namespace abtree

#endif /* STX_BTREE_HPP_ */
