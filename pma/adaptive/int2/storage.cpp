/*
 * storage.cpp
 *
 *  Created on: Mar 7, 2018
 *      Author: dleo@cwi.nl
 */

#include "storage.hpp"

#include <algorithm> // max
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <stdexcept>
#include "buffered_rewired_memory.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp" // hyperceil, get_memory_page_size
#include "rewired_memory.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[Storage::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace pma { namespace adaptive { namespace int2 {

Storage::Storage(size_t segment_size, size_t pages_per_extent) : m_segment_capacity(hyperceil(segment_size)), m_pages_per_extent(pages_per_extent) {
    if(hyperceil(segment_size ) > numeric_limits<uint16_t>::max()) throw std::invalid_argument("segment size too big, maximum is " + std::to_string( numeric_limits<uint16_t>::max() ));
    if(m_segment_capacity < 32) throw std::invalid_argument("segment size too small, minimum is 32");
    if(hyperceil(m_pages_per_extent) != m_pages_per_extent) throw std::invalid_argument("pages per extent must be a value from a power of 2");
    if(get_memory_page_size() % (m_segment_capacity * sizeof(m_keys[0])) != 0) throw std::invalid_argument("segment capacity must be a divisor of the virtual page size");

    m_capacity = m_segment_capacity;
    m_number_segments = 1;
    m_height = 1;
    m_cardinality = 0;

    alloc_workspace(1, &m_keys, &m_values, &m_segment_sizes, &m_memory_keys, &m_memory_values, &m_memory_sizes);
}

Storage::~Storage(){
    dealloc_workspace(&m_keys, &m_values, &m_segment_sizes, &m_memory_keys, &m_memory_values, &m_memory_sizes);
}

void Storage::extend(size_t num_segments_to_add){
    COUT_DEBUG("num_segments_to_add: " << num_segments_to_add << ", page size: " << get_memory_page_size());
    assert(m_memory_keys != nullptr);
    assert(m_memory_values != nullptr);
    assert(m_memory_sizes != nullptr);

    const size_t bytes_per_segment = m_segment_capacity * sizeof(m_keys[0]);
    constexpr size_t bytes_per_size = sizeof(m_segment_sizes[0]);
    const size_t bytes_per_extent = m_pages_per_extent * get_memory_page_size();

    size_t num_segments_before = m_number_segments;
    size_t num_segments_after = m_number_segments + num_segments_to_add;

    size_t elts_allocated_bytes = num_segments_before * bytes_per_segment;
    size_t elts_num_extents_current = (elts_allocated_bytes / bytes_per_extent) + ((elts_allocated_bytes % bytes_per_extent != 0));
    size_t elts_total_bytes = num_segments_after * bytes_per_segment;
    size_t elts_num_extents_total = (elts_total_bytes / bytes_per_extent) + ((elts_total_bytes % bytes_per_extent != 0)); // round up
    size_t elts_num_extents_required = elts_num_extents_total - elts_num_extents_current;

    size_t sizes_allocated_bytes = num_segments_before * bytes_per_size;
    size_t sizes_num_extents_current = (sizes_allocated_bytes / bytes_per_extent) + ((sizes_allocated_bytes % bytes_per_extent != 0));
    size_t sizes_total_bytes = num_segments_after * bytes_per_size;
    size_t sizes_num_extents_total = (sizes_total_bytes / bytes_per_extent) + ((sizes_total_bytes % bytes_per_extent != 0)); // round up
    size_t sizes_num_extents_required = sizes_num_extents_total - sizes_num_extents_current;

    COUT_DEBUG("[current] segments: " << num_segments_before << ", elts extents: " << elts_num_extents_current << ", card extents: " << sizes_num_extents_current);
    COUT_DEBUG("[after] segments: " << num_segments_after << ", elts extents: " << elts_num_extents_total << ", card extents: " << sizes_num_extents_total);

    if (elts_num_extents_required > 0){
        m_memory_keys->extend(elts_num_extents_required);
        m_memory_values->extend(elts_num_extents_required);
    }
    if(sizes_num_extents_required > 0){
        m_memory_sizes->extend(sizes_num_extents_required);
    }

    m_keys = (int64_t*) m_memory_keys->get_start_address();
    m_values = (int64_t*) m_memory_values->get_start_address();
    m_segment_sizes = (uint16_t*) m_memory_sizes->get_start_address();

    // update the properties
    m_number_segments = num_segments_after;
    m_capacity = m_number_segments * m_segment_capacity;
    m_height = log2(m_number_segments) +1;

}


void Storage::alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes, BufferedRewiredMemory** rewired_memory_keys, BufferedRewiredMemory** rewired_memory_values, RewiredMemory** rewired_memory_cardinalities){
    // reset the ptrs
    *keys = nullptr;
    *values = nullptr;
    *sizes = nullptr;
    *rewired_memory_keys = nullptr;
    *rewired_memory_values = nullptr;
    *rewired_memory_cardinalities = nullptr;

    // invoke dealloc_workspace on error
    auto onErrorDeleter = [&](void*){ dealloc_workspace(keys, values, sizes, rewired_memory_keys, rewired_memory_values, rewired_memory_cardinalities); };
    unique_ptr<Storage, decltype(onErrorDeleter)> onError{this, onErrorDeleter};

    const size_t extent_size = m_pages_per_extent * get_memory_page_size();
    const size_t elts_space_required_bytes = num_segments * m_segment_capacity * sizeof(m_keys[0]);
    const size_t card_space_required_bytes = max<size_t>(2, num_segments) * sizeof(m_segment_sizes[0]);
    bool use_rewired_memory = elts_space_required_bytes >= extent_size;

    if(use_rewired_memory){
        COUT_DEBUG("memory rewiring with " << num_segments << " segments (" << elts_space_required_bytes << " bytes)");
        const size_t elts_num_extents = elts_space_required_bytes / extent_size;
        assert(elts_num_extents >= 1);
        const size_t card_num_extents = max<size_t>(1, card_space_required_bytes / extent_size);

        *rewired_memory_keys = new BufferedRewiredMemory(m_pages_per_extent, elts_num_extents);
        *keys = (int64_t*) (*rewired_memory_keys)->get_start_address();
        *rewired_memory_values = new BufferedRewiredMemory(m_pages_per_extent, elts_num_extents);
        *values = (int64_t*) (*rewired_memory_values)->get_start_address();
        *rewired_memory_cardinalities = new RewiredMemory(m_pages_per_extent, card_num_extents, (*rewired_memory_keys)->get_max_memory() * sizeof(uint16_t) / sizeof(int64_t));
        *sizes = (uint16_t*) (*rewired_memory_cardinalities)->get_start_address();
    } else {
        COUT_DEBUG("posix_memalign with " << num_segments << " segments (" << elts_space_required_bytes << " bytes)");

        int rc(0);
        rc = posix_memalign((void**) keys, /* alignment */ 64,  /* size */ elts_space_required_bytes);
        if(rc != 0) {
            RAISE_EXCEPTION(Exception, "[Storage::alloc_workspace] It cannot obtain a chunk of aligned memory. " <<
                    "Requested size: " << elts_space_required_bytes);
        }
        rc = posix_memalign((void**) values, /* alignment */ 64,  /* size */ elts_space_required_bytes);
        if(rc != 0) {
            RAISE_EXCEPTION(Exception, "[Storage::alloc_workspace] It cannot obtain a chunk of aligned memory. " <<
                    "Requested size: " << elts_space_required_bytes);
        }

        rc = posix_memalign((void**) sizes, /* alignment */ 64,  /* size */ card_space_required_bytes);
        if(rc != 0) {
            RAISE_EXCEPTION(Exception, "[Storage::alloc_workspace] It cannot obtain a chunk of aligned memory. " <<
                    "Requested size: " << card_space_required_bytes);
        }
    }

    // always allocate at least 2 segments, using the second segment as special mark with size = 0
    // this makes easier to iterate on resize() by two at the time, without having to check the special case when only one segment is present
    (*sizes)[1] = 0;

    onError.release(); // avoid invoking dealloc_workspace, the memory has been (apparently) allocated
}

void Storage::dealloc_workspace(int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes, BufferedRewiredMemory** rewired_memory_keys, BufferedRewiredMemory** rewired_memory_values, RewiredMemory** rewired_memory_cardinalities){
    if(*rewired_memory_keys != nullptr){
        *keys = nullptr;
         delete *rewired_memory_keys; *rewired_memory_keys = nullptr;
    } else {
        free(*keys); *keys = nullptr;
    }
    if(*rewired_memory_values != nullptr){
        *values = nullptr;
         delete *rewired_memory_values; *rewired_memory_values = nullptr;
    } else {
        free(*values); *values = nullptr;
    }
    if(*rewired_memory_cardinalities != nullptr){
        *sizes = nullptr;
        delete *rewired_memory_cardinalities; *rewired_memory_cardinalities = nullptr;
    } else {
        free(*sizes); *sizes = nullptr;
    }
}

}}} // pma::adaptive::int1


