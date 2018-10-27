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

#ifndef PMA_V4_HPP_
#define PMA_V4_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

#include <cinttypes> // int64_t
#include <cstddef> // size_t
#include <utility> // pair

namespace pma {

class PMA_Impl4 : public Interface {
public:
    typedef int64_t key_t;
    typedef int64_t value_t;
    typedef std::pair<key_t, value_t> element_t;

private:
    element_t* m_elements; // the elements contained
    size_t m_capacity; // the total capacity of the array elementsS
    uint16_t* m_segments; // the number of segments;
    size_t m_segment_capacity; // the size of a single segment
    size_t m_height; // the height of the binary tree for elements
    size_t m_cardinality; // the number of elements contained

    element_t* m_workspace; // empty array used to spread elements

    static const size_t min_capacity = 8; // the minimum capacity of the PMA
    static constexpr double r_0 = 0.5; // highest threshold for the lower bound
    static constexpr double t_0 = 0.75; // lowest threshold for the upper bound
    static const size_t workspace_max_size = 1048576; // 1 M

    /**
     * Allocate the underlying memory array and initialize the PMA properties
     */
    void initialize(size_t capacity);

    /**
     * The total number of segments in the PMA
     */
    size_t get_num_segments() const;

    /**
     * Get the lower and upper segments for the given segment_id at the given height
     * Output:
     * - out_lb: the lower bound for the segment (inclusive)
     * - out_ub: the upper bound for the segment (inclusive)
     */
    void window_bounds(size_t pos, std::size_t height, int64_t& out_lb, int64_t& out_ub) const;

    /**
     * Compute the lower (out_a) and upper (out_b) threshold for the windows at the given `height'
     */
    void window_thresholds(std::size_t height, double& out_a, double& out_b) const;

    /**
     * Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
     */
    void rebalance(size_t segment_id);

    /**
     * Resize the storage & update the pointers in the index
     */
    void resize();

    /**
     * Equally spread the elements in the interval [window_start, window_start + window_length)
     */
    void spread(size_t num_elements, size_t window_start, size_t window_length);

    /**
     * Find the first element that is equal or greater than the given `key'
     * It returns the position of the element in `this->elements'. If there are no elements greater
     * than the given key, it returns `this->capacity'.
     */
    size_t find0(key_t key) const noexcept;

    /**
     * Find the segment where the given key should be placed
     */
    size_t find_segment(key_t key) const noexcept;

    /**
     * Insert the given pair <key, value> before the specified `position' in the array
     */
    void insert0(key_t key, value_t value, int64_t position);

public:
    PMA_Impl4();

    virtual ~PMA_Impl4();


    /**
     * Insert the given element in the PMA
     */
    void insert(key_t key, value_t value) override;

    /**
     * Find the element with the given key. It returns its value, or if not present -1.
     */
    virtual value_t find(key_t key) const noexcept override;

    /**
     * The number of elements stored
     */
    virtual size_t size() const override;

    /**
     * Check whether the data structure is empty
     */
    virtual bool empty() const;

    /**
     * Dump the content of the data structure to stdout (for debugging purposes)
     */
    virtual void dump() const override;

    /**
     * Iterator class
     */
    class Iterator : public pma::Iterator {
      friend class PMA_Impl4;
      const PMA_Impl4* m_instance; // attached instance
      std::size_t m_segment; // current segment
      std::size_t m_current; // current position (absolute)
      std::size_t m_stop; // end position in the segment (exclusive)
      std::size_t m_end; // end position (final)

      /**
       * Create the interator
       */
      Iterator(const PMA_Impl4* impl);

      /**
       * Move the cursor to the next available element
       */
      void move();

    public:
      /**
       * Check whether there is a next element in the iterator
       */
      virtual bool hasNext() const;

      /**
       * Retrieve the next element from the iterator
       */
      virtual std::pair<key_t, value_t> next();
    };
    friend class PMA_Impl4::Iterator;

    /**
     * Scan over all elements in the container
     */
    virtual std::unique_ptr<pma::Iterator> iterator() const override;

    /**
     * Sum all elements in [min, max]
     */
    pma::Interface::SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Remove all elements in the array
     */
    virtual void clear();
};

} // namespace pma

#endif /* PMA_V4_HPP_ */
