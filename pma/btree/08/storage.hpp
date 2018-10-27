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

#ifndef BTREE_08_STORAGE_HPP_
#define BTREE_08_STORAGE_HPP_

#include <cstddef>
#include <cstdint>

// forward declarations
class BufferedRewiredMemory;
class RewiredMemory;

namespace pma { namespace v8 {

// forward declarations
class Iterator;
class PackedMemoryArray8;
class SpreadWithRewiring;

class Storage {
    friend class Iterator;
    friend class PackedMemoryArray8;
    friend class SpreadWithRewiring;

    int64_t* m_keys; // pma for the keys
    int64_t* m_values; // pma for the values
    uint16_t* m_segment_sizes; // array, containing the cardinalities of each segment
    const uint16_t m_segment_capacity; // the max number of elements in a segment
    uint32_t m_cardinality; // the number of elements contained
    uint32_t m_number_segments; // the total number of segments, i.e. capacity / segment_size
    const size_t m_pages_per_extent; // number of virtual pages per extent, used in the RewiredMemory
    BufferedRewiredMemory* m_memory_keys = nullptr; // memory space used for the keys
    BufferedRewiredMemory* m_memory_values = nullptr; // memory space used for the values
    RewiredMemory* m_memory_sizes = nullptr; // memory space used for the segment cardinalities

public:
    Storage(uint64_t segment_size, uint64_t pages_per_extents);

    ~Storage();

    /**
     * Allocate the space to hold `num_segments'
     */
    void alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes, BufferedRewiredMemory** rewired_memory_keys, BufferedRewiredMemory** rewired_memory_values, RewiredMemory** rewired_memory_cardinalities);

    /**
     * Deallocate the space previously acquired with `alloc_workspace'
     */
    static void dealloc_workspace(int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes, BufferedRewiredMemory** rewired_memory_keys, BufferedRewiredMemory** rewired_memory_values, RewiredMemory** rewired_memory_cardinalities);

    /**
     * Extend the arrays for the keys/values/cardinalities by `num_segments' additional segments
     */
    void extend(size_t num_segments);

    /**
     * Shrink the arrays for the keys/values/cardinalities by `num_segments' segments
     */
    void shrink(size_t num_segment);

    /**
     * Insert the given pair in the segment. Return true if the key becomes the new minimum of the segment.
     * Precondition: the segment is neither full nor empty
     */
    bool insert(size_t segment_id, int64_t key, int64_t value) noexcept;

    /**
     * Retrieve the number of segments per extent
     */
    size_t get_segments_per_extent() const noexcept;

    /**
     * Retrieve the number of extents used for the keys/values
     */
    size_t get_number_extents() const noexcept;

    /**
     * Retrieve the height of the (incomplete) calibrator tree
     */
    int height() const noexcept;

    /**
     * Retrieve the height of the (full) calibrator tree
     */
    int hyperheight() const noexcept;

    /**
     * Retrieve the number of slots in the key/value array
     */
    size_t capacity() const noexcept;

    /**
     * Get the minimum of the given segment
     */
    int64_t get_minimum(size_t segment_id) const noexcept;

    /**
     * Retrieve the memory footprint used by the storage
     */
    size_t memory_footprint() const noexcept;
};

} /* namespace v8 */
} /* namespace pma */

#endif /* BTREE_08_STORAGE_HPP_ */
