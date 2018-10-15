/*
 * storage.hpp
 *
 *  Created on: Mar 7, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_INT1_STORAGE_HPP_
#define ADAPTIVE_INT1_STORAGE_HPP_

#include <cinttypes>
#include <cstddef>

namespace pma { namespace adaptive { namespace int1 {

struct Storage {
    int64_t* m_keys; // pma for the keys
    int64_t* m_values; // pma for the values
    uint16_t* m_segment_sizes; // array, containing the cardinalities of each segment
    const uint16_t m_segment_capacity; // the max number of elements in a segment
    uint16_t m_height; // the height of the binary tree for elements
    uint32_t m_cardinality; // the number of elements contained
    uint32_t m_capacity; // the size of the array elements
    uint32_t m_number_segments; // the total number of segments, i.e. capacity / segment_size

    // Initialise the PMA for a given segment size
    Storage(size_t segment_size);

    void alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes);
};


}}} // pma::adaptive::int1

#endif /* ADAPTIVE_INT1_STORAGE_HPP_ */
