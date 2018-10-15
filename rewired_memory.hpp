/*
 * rewired_memory.hpp
 *
 *  Created on: Jul 3, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef REWIRED_MEMORY_HPP_
#define REWIRED_MEMORY_HPP_

#include <cinttypes>
#include <cstddef>
#include <vector>

#include "errorhandling.hpp"

DEFINE_EXCEPTION(RewiredMemoryException);

/**
 * It represents a single large section of memory mapped memory. The memory is split in extents, multiple
 * of a virtual page. Extents within the mapped memory can be rewired, exchanging the mapping
 * between their virtual addresses and the underlying physical memory.
 */
class RewiredMemory{
    const size_t m_page_size; // virtual memory page size, for the underlying architecture
    const size_t m_num_pages_per_extent; // number of pages that compose an extent
    void* m_start_address; // the start address in virtual memory of the reserved region
    int m_handle_physical_memory; // the handle to the allocated physical memory, as file descriptor
    std::vector<uint32_t> m_translation_map; // an array, given an offset in virtual memory, returns the offset
    const size_t m_max_memory; // the maximum amount of virtual memory reserved for the memory mapping, in bytes

    /**
     * Raise an exception if the given address is not valid:
     * - it's not aligned to an extent
     * - it is not part of the memory space handled by this instance
     */
    void validate_address(void* address);
public:
    /**
     * Allocate a single segment of mapped memory
     * @param pages_per_extent it defines the size of a single extents, in terms of virtual pages
     * @param the amount of extents to allocate
     * @param max_memory the maximum amount of virtual memory that can be reserved by this instance, in bytes
     */
    RewiredMemory(size_t pages_per_extent, size_t num_extents, size_t max_memory = (1ull << 35) /* 2^35 = 32 GB */);

    /**
     * Destructor. Release the managed resources
     */
    ~RewiredMemory();

    /**
     * Retrieve the pointer to the virtual memory space
     */
    void* get_start_address() const noexcept;

    /**
     * Extent the amount of allocated memory
     */
    void extend(size_t num_extents);

    /**
     * Rewires the memory of addr1 and addr2, swapping their physical addresses
     */
    void swap(void* addr1, void* addr2);

    /**
     * The size of a single extent, in bytes
     */
    size_t get_extent_size() const noexcept;

    /**
     * Retrieve the amount of allocated memory, in bytes
     */
    size_t get_allocated_memory_size() const noexcept;

    /**
     * Retrieve the amount of allocated extents
     */
    size_t get_allocated_extents() const noexcept;

    /**
     * Retrieve the maximum amount of memory that can be allocated, in bytes
     */
    size_t get_max_memory() const noexcept;
};


#endif /* REWIRED_MEMORY_HPP_ */
