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
