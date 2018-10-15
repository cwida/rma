/*
 * storage.cpp
 *
 *  Created on: 9 Sep 2018
 *      Author: Dean De Leo
 */

#include "storage.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <memory>
#include "buffered_rewired_memory.hpp"
#include "miscellaneous.hpp"
#include "rewired_memory.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int3 {

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

/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

Storage::Storage(size_t segment_size, size_t pages_per_extent) : m_segment_capacity(hyperceil(segment_size)), m_pages_per_extent(pages_per_extent){
    if(hyperceil(segment_size ) > numeric_limits<uint16_t>::max()) throw std::invalid_argument("segment size too big, maximum is " + std::to_string( numeric_limits<uint16_t>::max() ));
    if(m_segment_capacity < 32) throw std::invalid_argument("segment size too small, minimum is 32");
    if(hyperceil(m_pages_per_extent) != m_pages_per_extent) throw std::invalid_argument("pages per extent must be a value from a power of 2");
    if(get_memory_page_size() % (m_segment_capacity * sizeof(m_keys[0])) != 0) throw std::invalid_argument("segment capacity must be a divisor of the virtual page size");

    m_number_segments = 1;
    m_cardinality = 0;

    // memory allocations
    alloc_workspace(1, &m_keys, &m_values, &m_segment_sizes, &m_memory_keys, &m_memory_values, &m_memory_sizes);
}

Storage::~Storage(){
    dealloc_workspace(&m_keys, &m_values, &m_segment_sizes, &m_memory_keys, &m_memory_values, &m_memory_sizes);
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
        const size_t elts_num_extents = elts_space_required_bytes / extent_size;
        assert(elts_num_extents >= 1);
        assert((elts_space_required_bytes % extent_size == 0) && "The number of segments must be a multiple of the extent size");
        const size_t card_num_extents = (card_space_required_bytes > extent_size) ?
                ((card_space_required_bytes/extent_size) + (card_space_required_bytes%extent_size > 0))
                : 1; // at least one segment for the cardinalities
        COUT_DEBUG("memory rewiring with " << num_segments << " segments (" << elts_space_required_bytes << " bytes). Using " << elts_num_extents << " extents for the keys/values and " << card_num_extents << " extents for the cardinalities. Extent size: " << extent_size << " bytes.");

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
    size_t sizes_preallocated_extents = m_memory_sizes->get_allocated_extents(); // we may have shrunk the array before
    size_t sizes_num_extents_required = 0;
    if(sizes_preallocated_extents < sizes_num_extents_total){
        sizes_num_extents_required = min(sizes_num_extents_total - sizes_num_extents_current, sizes_num_extents_total - sizes_preallocated_extents);
    }


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
}

void Storage::shrink(size_t num_segments_to_remove){
    COUT_DEBUG("num_segments_to_remove: " << num_segments_to_remove << ", page size: " << get_memory_page_size());
    if(num_segments_to_remove == 0) return; // nop

    assert(m_memory_keys != nullptr);
    assert(m_memory_values != nullptr);
    assert(m_memory_sizes != nullptr);
    assert(num_segments_to_remove % get_segments_per_extent() == 0 && "The number of segments to remove must be a multiple of segments per page");

    size_t num_segments_before = m_number_segments;
    assert(num_segments_to_remove < m_number_segments && "Do you want to remove all the segments?");
    size_t num_segments_after = m_number_segments - num_segments_to_remove;

    const size_t segments_per_extent = get_segments_per_extent();
    size_t elts_num_extents_before = num_segments_before / segments_per_extent;
    size_t elts_num_extents_after = num_segments_after / segments_per_extent;
    size_t elts_num_extents_to_release = elts_num_extents_before - elts_num_extents_after;

    if (elts_num_extents_to_release > 0){
        m_memory_keys->shrink(elts_num_extents_to_release);
        m_memory_values->shrink(elts_num_extents_to_release);
    }

    // we cannot shrink the array sizes

    // update the properties
    m_number_segments = num_segments_after;
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


/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/
size_t Storage::get_segments_per_extent() const noexcept {
    const size_t extent_size_bytes = m_pages_per_extent * get_memory_page_size();
    const size_t segment_size_bytes = m_segment_capacity * sizeof(int64_t);
    assert(extent_size_bytes % segment_size_bytes == 0);
    return extent_size_bytes / segment_size_bytes;
}

size_t Storage::get_number_extents() const noexcept {
    return m_memory_keys != nullptr ? m_memory_keys->get_allocated_extents() - m_memory_keys->get_total_buffers() : 1;
}

int Storage::height() const noexcept {
    return floor(log2(m_number_segments)) +1;
}

int Storage::hyperheight() const noexcept {
    return ceil(log2(m_number_segments)) +1;
}

size_t Storage::capacity() const noexcept {
    return m_number_segments * m_segment_capacity;
}

int64_t Storage::get_minimum(size_t segment_id) const noexcept {
    int64_t* __restrict keys = m_keys;
    auto* __restrict sizes = m_segment_sizes;

    assert(segment_id < m_number_segments && "Invalid segment");
    assert(sizes[segment_id] > 0 && "The segment is empty!");

    if(segment_id % 2 == 0){ // even segment
        return keys[(segment_id +1) * m_segment_capacity - sizes[segment_id]];
    } else { // odd segment
        return keys[segment_id * m_segment_capacity];
    }
}

size_t Storage::memory_footprint() const noexcept {
    size_t memory_keys = m_memory_keys != nullptr ? m_memory_keys->get_allocated_memory_size() : capacity() * sizeof(m_keys[0]);
    size_t memory_values = m_memory_values != nullptr ? m_memory_values->get_allocated_memory_size() : capacity() * sizeof(m_values[0]);
    size_t memory_sizes = m_memory_sizes != nullptr ? m_memory_sizes->get_allocated_memory_size() : capacity() * sizeof(m_segment_sizes[0]);
    return memory_keys + memory_values + memory_sizes;
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/

bool Storage::insert(size_t segment_id, int64_t key, int64_t value, int64_t* out_predecessor, int64_t* out_successor) noexcept {
    assert(m_segment_sizes[segment_id] < m_segment_capacity && "This segment is full!");

    int64_t* __restrict keys = m_keys + segment_id * m_segment_capacity;
    int64_t* __restrict values = m_values + segment_id * m_segment_capacity;
    bool minimum = false; // the inserted key is the new minimum ?
    size_t sz = m_segment_sizes[segment_id];

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), insert at the end of the segment
        size_t stop = m_segment_capacity -1;
        size_t start = m_segment_capacity - sz -1;
        size_t i = start;

        while(i < stop && keys[i+1] < key){
            keys[i] = keys[i+1];
            i++;
        }

//        COUT_DEBUG("(even) segment_id: " << segment_id << ", start: " << start << ", stop: " << stop << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = start; j < i; j++){
            values[j] = values[j+1];
        }
        values[i] = value;

        minimum = (i == start);
        bool maximum = (i == stop);

        // for the detector
        if(out_predecessor) { *out_predecessor = minimum ? std::numeric_limits<int64_t>::min() : keys[i -1]; }
        if(out_successor) { *out_successor = maximum ? std::numeric_limits<int64_t>::max() : keys[i +1]; }
    } else { // for odd segment ids (1, 3, ...), insert at the front of the segment
        size_t i = sz;
        while(i > 0 && keys[i-1] > key){
            keys[i] = keys[i-1];
            i--;
        }

//        COUT_DEBUG("(odd) segment_id: " << segment_id << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = sz; j > i; j--){
            values[j] = values[j-1];
        }
        values[i] = value;

        minimum = (i == 0);
        bool maximum = (i == sz);

        // for the detector
        if(out_predecessor) { *out_predecessor = minimum ? std::numeric_limits<int64_t>::min() : keys[i -1]; }
        if(out_successor) { *out_successor = maximum ? std::numeric_limits<int64_t>::max() : keys[i +1]; }
    }

    // update the cardinality
    m_segment_sizes[segment_id]++;
    m_cardinality += 1;

    return minimum;
}

}}} // pma::adaptive::int3
