/*
 * interface.hpp
 *
 *  Created on: 17 Jul 2017
 *      Author: Dean De Leo
 */

#ifndef PMA_INTERFACE_HPP_
#define PMA_INTERFACE_HPP_

#include <cinttypes>
#include <memory>
#include <ostream>
#include <utility>

namespace pma {

// Forward declaration
struct Iterator;

/**
 * Common interface shared by all PMA implementations. This interface
 * is used to run the simulations against a single API. The methods that
 * an implementation should provide are:
 * - insert(key, value): insert a new element in the data structure
 * - find(key) -> value: retrieve the value of the given key
 * - [optional] remove(key) -> value: remove an element from the data structure, return its value
 * - sum(min, max) -> SumResult: emulate a range query in the interval [min, max], aggregate and sum all qualifying elements
 */
class Interface {
public:

    /**
     * Resulting data structure to emulate a scan in an arbitrary range [min, max].
     */
    struct SumResult {
        int64_t m_first_key =0; // the first key that qualifies inside the interval [min, max]. Undefined if m_num_elements == 0
        int64_t m_last_key =0; // the last key that qualifies inside the interval [min, max]. Undefined if m_num_elements == 0
        uint64_t m_num_elements =0; // the total number of elements inside the interval [min, max]
        int64_t m_sum_keys =0; // the aggregate sum of all keys in the interval [min, max]
        int64_t m_sum_values =0; // the aggregate sum of all values in the interval [min, max]
    };

    /**
     * Virtual destructor
     */
    virtual ~Interface();

    /**
     * Insert the given <key, value> in the container
     */
    virtual void insert(int64_t key, int64_t value) = 0;

    /**
     * Invoked by the experiments after a batch of inserts. By default this is a dummy method that
     * does nothing, but some implementation may have a special behaviour. For instance, the baseline
     * ABtree data structure uses this method to randomly swap its nodes in memory when the parameter
     * --abtree_random_permutation is set. The purpose is to simulate an aging tree.
     */
    virtual void build();

    /**
     * Return the value associated to the element with the given `key', or -1 if not present.
     * In case of duplicates, it returns the value of one of the qualifying elements.
     */
    virtual int64_t find(int64_t key) const = 0;

    /**
     * Remove the element with the given `key' from the PMA. Supported only by few implementations.
     * Returns the value associated to the given `key', or -1 if not found.
     */
    virtual int64_t remove(int64_t key);

    /**
     * Emulate a scan in the range [min, max]. Sum all keys and values together for the elements
     * that are in the given range.
     */
    virtual SumResult sum(int64_t min, int64_t max) const = 0;

    /**
     * Scan all elements in the container
     */
    virtual std::unique_ptr<Iterator> iterator() const = 0;

    /**
     * Return the number of elements in the container
     */
    virtual std::size_t size() const = 0;

    /**
     * Return the space footprint (in bytes) of the whole data structure
     */
    virtual std::size_t memory_footprint() const;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    virtual void dump() const = 0;
};

/**
 * [Interface]
 * Interface to perform range queries, however this is not used anymore by
 * the experiments, and it only serves the purpose of debugging.
 * In the benchmarks, it has been replaced by the method sum(min, max), where the
 * sum of both keys and values in the given range [min, max] is performed in place.
 *
 */
class InterfaceRQ : public Interface {
public:
    /**
     * Destructor
     */
    virtual ~InterfaceRQ();

    /**
     * Find all elements in the interval [min, max]
     */
    virtual std::unique_ptr<Iterator> find(int64_t min, int64_t max) const = 0;

    /**
     * Scan all elements in the container
     */
    virtual std::unique_ptr<Iterator> iterator() const;
};


std::ostream& operator<<(std::ostream& out, const Interface::SumResult& sum);

} // namespace pma

#endif /* PMA_INTERFACE_HPP_ */
