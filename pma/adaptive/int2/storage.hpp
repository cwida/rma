/*
 * storage.hpp
 *
 *  Created on: 4 Jul 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_INT2_STORAGE_HPP_
#define ADAPTIVE_INT2_STORAGE_HPP_

#include <cinttypes>
#include <cstddef>

class RewiredMemory; // forward decl.
class BufferedRewiredMemory; // forward decl.

namespace pma { namespace adaptive { namespace int2 {

struct Storage {
    int64_t* m_keys; // pma for the keys
    int64_t* m_values; // pma for the values
    uint16_t* m_segment_sizes; // array, containing the cardinalities of each segment
    const uint16_t m_segment_capacity; // the max number of elements in a segment
    uint16_t m_height; // the height of the binary tree for elements
    uint32_t m_cardinality; // the number of elements contained
    uint32_t m_capacity; // the size of the array elements
    uint32_t m_number_segments; // the total number of segments, i.e. capacity / segment_size
    const size_t m_pages_per_extent; // number of virtual pages per extent, used in the RewiredMemory
    BufferedRewiredMemory* m_memory_keys; // memory space used for the keys
    BufferedRewiredMemory* m_memory_values; // memory space used for the values
    RewiredMemory* m_memory_sizes; // memory space used for the segment cardinalities

    // Initialise the PMA for a given segment size
    Storage(size_t segment_size, size_t pages_per_extent);

    // Clean up
    ~Storage();

    /**
     * Extend the arrays for the keys/values/cardinalities by `num_segments' additional segments
     */
    void extend(size_t num_segments);

    void alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes, BufferedRewiredMemory** rewired_memory_keys, BufferedRewiredMemory** rewired_memory_values, RewiredMemory** rewired_memory_cardinalities);

    static void dealloc_workspace(int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes, BufferedRewiredMemory** rewired_memory_keys, BufferedRewiredMemory** rewired_memory_values, RewiredMemory** rewired_memory_cardinalities);
};


}}} // pma::adaptive::int2

#endif /* ADAPTIVE_INT2_STORAGE_HPP_ */
