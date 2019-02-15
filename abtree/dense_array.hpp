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

#ifndef DENSE_ARRAY_HPP_
#define DENSE_ARRAY_HPP_

#include <cinttypes> // fixed size scalar types
#include <cstddef> // size_t
#include <vector>

#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "pma/generic/static_index.hpp"

namespace abtree {

class DenseArray: public pma::InterfaceRQ {
private:
    pma::StaticIndex m_index; // the index for the elements in the array
    int64_t* m_keys; // a dense static array containing the ordered sequence of keys
    int64_t* m_values; // a dense static array containing the ordered sequence of values
    uint64_t m_cardinality; // the current number of elements stored in the dense arrays (it doesn't take into account the delta)
    using delta_t = std::vector<std::pair<int64_t, int64_t>>;
    delta_t m_delta; // delta (temporary) storage, elements inserted but not yet merged in the static dense array.
    int m_handle_physical_memory_keys = -1; // the handle to the physical memory for the allocated storage that backs the array `m_keys'
    int m_handle_physical_memory_values = -1; // as above, for the array `m_values'

    // NB: the delta is merged only when explicitly invoking the method ::build(). In this implementation elements in the
    // delta are NOT sorted, they do not participate in scans and are not searched in. A delta here only represents a
    // temporary buffer for elements to be stored in the static dense arrays m_keys and m_values.

    void acquire_memory(int* out_handle_keys, int* out_handle_values, int64_t** out_array_keys, int64_t** out_array_values, uint64_t total_cardinality);

    void release_memory(int& handle_keys, int& handle_values, int64_t*& array_keys, int64_t*& array_values, uint64_t cardinality);

    // Retrieve the amount of memory needed, in bytes, to store `cardinality' keys or values, such that the given quantity is aligned
    // to a page boundary.
    static uint64_t get_amount_memory_needed(uint64_t cardinality);

    // Implementation of the iterator class
    class InternalIterator : public pma::Iterator {
        int64_t* m_keys; // a dense static array containing the ordered sequence of keys
        int64_t* m_values; // a dense static array containing the ordered sequence of values
        uint64_t m_offset; // current position
        uint64_t m_end; // final position

    public:
        /**
         * Perform a scan in [begin_incl, end_excl)
         */
        InternalIterator(const DenseArray* instance, size_t begin_incl, size_t end_excl);

        /**
         * Check whether a next element exists
         */
        bool hasNext() const override;

        /**
         * Return the next element
         */
        std::pair<int64_t, int64_t> next() override;
    };

public:
    /**
     * Initialise an empty dense array
     * @param node_size the capacity of the nodes, in terms of maximum number of separator keys, in the static index
     */
    DenseArray(size_t node_size = 64);

    /**
     * Destructor
     */
    virtual ~DenseArray();

    /**
     * Append the given <key, value> in the delta
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Return the number of elements in the static tree, without taking into account the delta
     */
    std::size_t size() const override;

    /**
     * Check whether the data structure is empty, withouth taking into account the delta
     */
    bool empty() const;

    /**
     * Rebuild the dense arrays by merging the current elements with the elements in the delta
     */
    void build() override;

    /**
     * Return the value associated to the element with the given `key', or -1 if not present.
     * In case of duplicates, it returns the value of one of the qualifying elements.
     */
    int64_t find(int64_t key) const override;

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
     * Report the memory footprint, in bytes, of the dense arrays and the above index. The delta is not taken into account.
     */
    size_t memory_footprint() const override;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    void dump() const override;
};

} /* namespace abtree */

#endif /* DENSE_ARRAY_HPP_ */
