/*
 * buffered_rewired_memory.cpp
 *
 *  Created on: Jul 12, 2018
 *      Author: dleo@cwi.nl
 */


#include "buffered_rewired_memory.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>

#include "errorhandling.hpp"

using namespace std;

#define RAISE(msg) RAISE_EXCEPTION(RewiredMemoryException, msg)

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[BufferedRewiredMemory::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

BufferedRewiredMemory::BufferedRewiredMemory(size_t pages_per_extent, size_t num_extents) :
        m_instance(pages_per_extent, num_extents),
        m_buffer_start_address(static_cast<char*>(m_instance.get_start_address()) + m_instance.get_allocated_memory_size()),
        m_allocated_buffers(0)
        { }


/*****************************************************************************
 *                                                                           *
 *   Handle buffer space                                                     *
 *                                                                           *
 *****************************************************************************/
void BufferedRewiredMemory::add_buffers(size_t num_extents){
    m_instance.extend(num_extents);

    // register the new buffers
    char* buffer_space = static_cast<char*>(m_buffer_start_address) + get_extent_size() * get_total_buffers();
    for(size_t i = 0; i < num_extents; i++){
        m_buffers.push_front(buffer_space + i * get_extent_size());
    }

    // update the state of the data structure
    m_allocated_buffers += num_extents;

    COUT_DEBUG("acquired " << num_extents << " extents. Total buffer capacity: " << get_total_buffers() << " extents");
}

void* BufferedRewiredMemory::acquire_buffer(){
    if(m_buffers.empty()){ add_buffers(max<size_t>(4, m_allocated_buffers * 0.5)); }
    assert(!m_buffers.empty());
    void* address = m_buffers.back();
    m_buffers.pop_back();
    COUT_DEBUG("address: " << address);
    return address;
}

void BufferedRewiredMemory::swap_and_release(void* addr1, void* addr2){
    // check whether addr1 or addr2 is the pointer to the buffer
    char* ptr_bufferspace (nullptr);
    char* ptr_userspace (nullptr);

    char* vmem1 = (char*) addr1;
    char* vmem2 = (char*) addr2;
    char* start_address_buffers = (char*) m_buffer_start_address;

    if(vmem1 >= start_address_buffers){
        ptr_bufferspace = vmem1;
        ptr_userspace = vmem2;
    }
    if(vmem2 >= start_address_buffers){
        // already set
        if(ptr_bufferspace != nullptr){
            RAISE("both pointers refer to buffers: addr1: " << addr1 << ", addr2: " << addr2 << ", buffer start address: " << (void*) start_address_buffers);
        }
        ptr_bufferspace = vmem2;
        ptr_userspace = vmem1;
    }
    // both addresses do not refer to the buffer space
    if(ptr_bufferspace == nullptr){
        RAISE("both pointers do not refer to a buffer: addr1: " << addr1 << ", addr2: " << addr2 << ", buffer start address: " << (void*) start_address_buffers);
    }
    COUT_DEBUG("userspace: " << (void*) ptr_userspace << ", bufferspace: " << (void*) ptr_bufferspace);

    m_instance.swap(ptr_userspace, ptr_bufferspace);
    m_buffers.push_back(ptr_bufferspace);
}

/*****************************************************************************
 *                                                                           *
 *   Resize                                                                  *
 *                                                                           *
 *****************************************************************************/
void BufferedRewiredMemory::extend(size_t num_extents){
    if(num_extents == 0) RAISE("The amount of extents specified is zero");
    assert(get_used_buffers() == 0 && "There are buffers in use!");
    if(get_used_buffers() != 0) RAISE("There are buffers in use: " << get_used_buffers() << "/" << get_total_buffers());

    // the buffers are at the end of
    int64_t num_extents_buffer = get_total_buffers();
    int64_t additional_phys_memory = static_cast<int64_t>(num_extents) - num_extents_buffer;

    if(additional_phys_memory < 0){ // there is no need to extent the physical memory, just cover it with the buffer space
        const size_t extent_size = get_extent_size();
        m_buffer_start_address = ((char*) m_buffer_start_address) + num_extents * extent_size;
        m_allocated_buffers = num_extents_buffer - num_extents;
        m_buffers.clear(); // rebuild the deque
        char* buffer_address = (char*) m_buffer_start_address;
        for(size_t i = 0; i < m_allocated_buffers; i++){
            m_buffers.push_front(buffer_address);
            buffer_address += extent_size;
        }
    } else { // we need to acquire more physical memory
        m_instance.extend(additional_phys_memory);

        // all the space previously occupied by the buffer space is now in use for the user data
        m_allocated_buffers = 0;
        m_buffers.clear();

        m_buffer_start_address = static_cast<char*>(m_instance.get_start_address()) + m_instance.get_allocated_memory_size();
    }
}

void BufferedRewiredMemory::shrink(size_t num_extents){
    if(num_extents == 0) RAISE("The amount of extents specified is zero");
    assert(get_used_buffers() == 0 && "There are buffers in use!");
    if(get_used_buffers() != 0) RAISE("There are buffers in use: " << get_used_buffers() << "/" << get_total_buffers());
    if(num_extents > get_allocated_extents() - get_total_buffers()) RAISE("Releasing more memory than acquired");

    char* buffer_address = (char*) m_buffer_start_address;
    const size_t extent_size = get_extent_size();
    for(size_t i = 0; i < num_extents; i++){
        buffer_address -= extent_size; // in bytes
        m_buffers.push_front(buffer_address);
    }
    m_allocated_buffers += num_extents;
    m_buffer_start_address = buffer_address;
}


/*****************************************************************************
 *                                                                           *
 *   Observers                                                               *
 *                                                                           *
 *****************************************************************************/

void* BufferedRewiredMemory::get_start_address() const noexcept {
    return m_instance.get_start_address();
}

size_t BufferedRewiredMemory::get_extent_size() const noexcept{
    return m_instance.get_extent_size();
}

size_t BufferedRewiredMemory::get_allocated_extents() const noexcept {
    return m_instance.get_allocated_extents();
}

size_t BufferedRewiredMemory::get_allocated_memory_size() const noexcept{
    return m_instance.get_allocated_memory_size();
}

size_t BufferedRewiredMemory::get_total_buffers() const noexcept {
    return m_allocated_buffers;
}

size_t BufferedRewiredMemory::get_used_buffers() const noexcept{
    assert(m_buffers.size() <= m_allocated_buffers && "The total number of free buffers must be less or equal those allocated");
    return m_allocated_buffers - m_buffers.size();
}

size_t BufferedRewiredMemory::get_max_memory() const noexcept{
    return m_instance.get_max_memory();
}
