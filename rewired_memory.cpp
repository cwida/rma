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

#include "rewired_memory.hpp"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <linux/memfd.h>
#include <string>
#include <sys/mman.h> // mmap
#include <unistd.h>
#include "configuration.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp"

using namespace std;

#define RAISE(msg) RAISE_EXCEPTION(RewiredMemoryException, msg)

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[RewiredMemory::" << __FUNCTION__ << "] " << msg << std::endl
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
static int g_internal_id = 0;

RewiredMemory::RewiredMemory(size_t pages_per_extent, size_t num_extents, size_t max_memory) :
        m_page_size(get_memory_page_size()), m_num_pages_per_extent(pages_per_extent), m_start_address(nullptr),
        m_handle_physical_memory(-1), m_max_memory(max_memory){
    // validate the user parameters
    if(pages_per_extent <= 0){ throw invalid_argument("[RewiredMemory::ctor] pages_per_extent <= 0"); }
    if(num_extents <= 0){ throw invalid_argument("[RewiredMemory::ctor] num_extents <= 0"); }
    int rc = 0;

    // check whether we are allowed to allocate the amount of memory requested
    size_t size_physical_memory = get_extent_size() * num_extents;
    if(size_physical_memory > get_max_memory()){
        RAISE("Cannot allocate " << (size_physical_memory) << " bytes. The maximum amount of reserved virtual memory specified for this instance is: " << get_max_memory() << " bytes.");
    }

    // create the handle to the physical memory
    string id = "rewired_memory_";
    id += to_string(g_internal_id++);
    m_handle_physical_memory = memfd_create(id.c_str(), configuration::use_huge_pages() ? MFD_HUGETLB : 0); // miscellaneous.hpp
    if(m_handle_physical_memory < 0){ RAISE("Cannot allocate the physical memory. memfd_create error: " << strerror(errno) << "(" << errno << ")"); }

    // allocate the physical memory
    COUT_DEBUG("Pages per extent: " << pages_per_extent << ", num_extents: " << num_extents << ", extent size: " << get_extent_size() << " bytes, physical memory requested: " << size_physical_memory << ", virtual memory reserved: " << get_max_memory() << " bytes");
    rc = ftruncate(m_handle_physical_memory, size_physical_memory);
    if(rc != 0){ RAISE("Cannot allocate the physical memory. ftruncate error: " << strerror(errno) << "(" << errno << ")"); }

    // memory map the physical memory to a virtual address
    void* mmap_ret = mmap(
        /* starting address, NULL means arbitrary */ NULL,
        /* length in bytes */ get_max_memory(),
        /* memory protection */ PROT_READ | PROT_WRITE,
        /* flags */ MAP_SHARED,
        /* file descriptor */ m_handle_physical_memory,
        /* offset, in terms of multiples of the page size */ 0);
    if(mmap_ret == MAP_FAILED){ RAISE("Cannot allocate the virtual memory: " << get_max_memory() << " bytes. mmap error: " << strerror(errno) << "(" << errno << ")"); }
    m_start_address = mmap_ret;
    /**
     * In case the user attempts to access mapped memory not backed by the physical memory (as m_allocated_extents < m_reserved_extents)
     * the kernel will throw a SIGBUS interruption
     */

    // create the translation map
    m_translation_map.reserve(num_extents);
    for(size_t i = 0; i < num_extents; i++){
        m_translation_map.push_back(i);
    }
}


RewiredMemory::~RewiredMemory(){
    // release the managed virtual memory
    if(m_start_address != nullptr){
        int rc = munmap(m_start_address, get_max_memory());
        if(rc < 0){
            cerr << "[RewiredMemory::dtor] Error in releasing the virtual memory, munmap error: " << strerror(errno) << " (" << errno << ")" << endl;
        }
    }

    // release the acquired physical memory
    if(m_handle_physical_memory >= 0){
        int rc = close(m_handle_physical_memory);
        if(rc < 0){
            cerr << "[RewiredMemory::dtor] Error in releasing the physical memory, fh: " << m_handle_physical_memory << ": " << strerror(errno) << " (" << errno << ")" << endl;
        }
        m_handle_physical_memory = -1;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Memory rewiring                                                         *
 *                                                                           *
 *****************************************************************************/

void RewiredMemory::validate_address(void* address){
    if(((uint64_t) address - (uint64_t) m_start_address) % get_extent_size() != 0){ RAISE("Address not aligned to the extent: " << address); }
    char* start_address = (char*) get_start_address();
    char* vmem = (char*) address;
    if(vmem < start_address || vmem >= start_address + get_allocated_memory_size() ){
        RAISE("Invalid address: `" << (void*) vmem << "' is not mapped. Start address: " << (void*) get_start_address() << ", " <<
                "end address: " << (void*) (start_address + get_allocated_memory_size()));
    }
}

void RewiredMemory::swap(void* addr1, void* addr2){
    COUT_DEBUG("address 1: " << addr1 << ", address 2: "  << addr2);

    validate_address(addr1);
    validate_address(addr2);
    if(addr1 == addr2){ RAISE("The arguments addr1 and addr2 are the same: " << addr1); }

    char* vpage1 = (char*) addr1;
    char* vpage2 = (char*) addr2;
    char* start_address = (char*) get_start_address();

    size_t trmap_off1 = (vpage1 - start_address) / get_extent_size();
    size_t ppage1 = m_translation_map[trmap_off1];
    size_t trmap_off2 = (vpage2 - start_address) / get_extent_size();
    size_t ppage2 = m_translation_map[trmap_off2];
    COUT_DEBUG("vpage1: " << addr1 << ", ppage1: " << ppage1 << ", vpage2: " << addr2 << ", ppage2: " << ppage2);

    void* mmap_ret = nullptr;

    // set ppage2 to vpage1
    mmap_ret = mmap(
            /* destination (virtual address) */ vpage1, get_extent_size(),
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_FIXED,
            /* source (physical location) */ m_handle_physical_memory, ppage2 * get_extent_size()
    );
    if(mmap_ret == MAP_FAILED){
        cerr << "[RewiredMemory::swap] first rewiring failed, start_address: " << (void*) get_start_address() << ", extent size: " << get_extent_size() << ", allocated space: " << get_allocated_memory_size() << " bytes" << endl;
        RAISE("first rewiring failed: " << (void*) vpage1 << ", " << strerror(errno) << " (" << errno << ")");
    }
    // set ppage 1 to vpage2
    mmap_ret = mmap(
            /* destination (virtual address) */ vpage2, get_extent_size(),
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_FIXED,
            /* source (physical location) */ m_handle_physical_memory, ppage1 * get_extent_size()
    );
    if(mmap_ret == MAP_FAILED){
        cerr << "[RewiredMemory::swap] second rewiring failed, start_address: " << (void*) get_start_address() <<", extent size: " << get_extent_size() << ", allocated space: " << get_allocated_memory_size() << " bytes" << endl;
        RAISE("second rewiring failed: " << (void*) vpage2 << ", " << strerror(errno) << " (" << errno << ")");
    }

    m_translation_map[trmap_off1] = ppage2;
    m_translation_map[trmap_off2] = ppage1;
}


void RewiredMemory::extend(size_t num_extents){
    if(num_extents == 0) return;
    size_t memory_in_bytes = get_allocated_memory_size() +  num_extents * get_extent_size();
    if(memory_in_bytes > get_max_memory()){
       RAISE("Already reached the limit for the maximum size that can be allocated: " << get_max_memory() << " bytes. "
               "Allocated size: " << get_allocated_memory_size() << " bytes, requested size: " << memory_in_bytes);
    }

    int rc = ftruncate(m_handle_physical_memory, memory_in_bytes);
    if(rc != 0){ RAISE("Cannot allocate the physical memory: " << memory_in_bytes << " bytes. ftruncate error: " << strerror(errno) << "(" << errno << ")"); }

    size_t start_fd = m_translation_map.size();
    m_translation_map.reserve(get_allocated_extents() + num_extents);
    for(size_t i = 0; i < num_extents; i++){
        m_translation_map.push_back(start_fd +i);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Observers                                                               *
 *                                                                           *
 *****************************************************************************/
void* RewiredMemory::get_start_address() const noexcept {
    return m_start_address;
}

size_t RewiredMemory::get_extent_size() const noexcept {
    return m_page_size * m_num_pages_per_extent;
}

size_t RewiredMemory::get_allocated_extents() const noexcept {
    return m_translation_map.size();
}

size_t RewiredMemory::get_allocated_memory_size() const noexcept {
    return get_extent_size() * get_allocated_extents();
}

size_t RewiredMemory::get_max_memory() const noexcept {
    return m_max_memory;
}

