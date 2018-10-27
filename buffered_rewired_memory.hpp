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

#ifndef BUFFERED_REWIRED_MEMORY_HPP_
#define BUFFERED_REWIRED_MEMORY_HPP_

#include <deque>

#include "rewired_memory.hpp"

class BufferedRewiredMemory {
    RewiredMemory m_instance;
    void* m_buffer_start_address;
    size_t m_allocated_buffers; // the total number of allocated buffers,
    std::deque<void*> m_buffers; // list of free virtual addresses that can be acquired for buffering

    /**
     * Extend the physical memory to make available additional buffers
     */
    void add_buffers(size_t num_buffers);

public:
    /**
     * It allocates a chunk of rewired memory
     */
    BufferedRewiredMemory(size_t pages_per_extent, size_t num_extents);

    /**
     * Get a buffer from the free buffer space. A single buffer has the size of an extent.
     */
    void* acquire_buffer();

    /**
     * Rewires the memory of addr1 and addr2. Moreover, it assumes that either addr1
     * or addr2 (but not both) is a pointer to the buffer space. After the memory
     * is rewiring, the buffer space pointer is reclaimed as free buffer space.
     */
    void swap_and_release(void* addr1, void* addr2);

    /**
     * Extend the amount of memory available. No buffers must be in use
     */
    void extend(size_t num_extents);

    /**
     * Shrink the number of extents in use. The actual physical memory is not released but recycled as buffer space.
     * Precondition: no buffers must be in use.
     */
    void shrink(size_t num_extents);

    /**
     * Retrieve the pointer to the allocated virtual memory space
     */
    void* get_start_address() const noexcept;

    /**
     * Retrieve the size of a single extent, in bytes
     */
    size_t get_extent_size() const noexcept;

    /**
     * Retrieve the amount of allocated extents
     */
    size_t get_allocated_extents() const noexcept;

    /**
     * Retrieve the total amount of allocated physical memory
     */
    size_t get_allocated_memory_size() const noexcept;

    /**
     * Retrieve the total number of buffers allocated, a single buffer corresponds to an extent
     */
    size_t get_total_buffers() const noexcept;

    /**
     * Retrieve the total number of buffers that are still in use
     */
    size_t get_used_buffers() const noexcept;

    /**
     * Total amount of reserved memory
     */
    size_t get_max_memory() const noexcept;
};
//};


#endif /* BUFFERED_REWIRED_MEMORY_HPP_ */
