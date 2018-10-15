/*
 * spread_with_rewiring.cpp
 *
 *  Created on: Sep 6, 2018
 *      Author: dleo@cwi.nl
 */

#include "spread_with_rewiring.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include "buffered_rewired_memory.hpp"
#include "errorhandling.hpp"
#include "packed_memory_array.hpp"
#include "rewired_memory.hpp"
#include "storage.hpp"

using namespace std;

//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[SpreadWithRewiring::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace pma { namespace v8 {

SpreadWithRewiring::SpreadWithRewiring(PackedMemoryArray8* instance, size_t window_start, size_t window_length, size_t cardinality)
    : m_instance(*instance), m_window_start(window_start), m_window_length(window_length), m_cardinality(cardinality),
      m_segments_per_extent(m_instance.m_storage.m_memory_keys->get_extent_size() / (m_instance.m_storage.m_segment_capacity * sizeof(uint64_t))) {
    auto segment_capacity = get_segment_capacity();
    int64_t window_end = m_window_start + m_window_length -1;
    m_position = window_end * segment_capacity + m_instance.m_storage.m_segment_sizes[window_end];
}

void SpreadWithRewiring::set_element_to_insert(int64_t key, int64_t value){
    if(m_insert){
        RAISE_EXCEPTION(Exception, "[SpreadWithRewiring::set_key_to_insert] A key to insert has already been set: <" << m_insert_key << ", " << m_insert_value << ">");
    }

    m_insert = true;
    m_insert_key = key;
    m_insert_value = value;
}

void SpreadWithRewiring::set_start_position(size_t position){
//    // check that position is inside the current window
//    int64_t segment_id = position2segment(static_cast<int64_t>(position) -1);
//    int64_t window_start = m_window_start;
//    int64_t window_end = m_window_start + m_window_length;
//    if(segment_id < window_start || segment_id >= window_end){
//        RAISE_EXCEPTION(Exception, "Invalid starting position: " << position << ", segment: " << segment_id << ". Invalid from the current window: [" << window_start << ", " << window_end << ").");
//    }
//    COUT_DEBUG("position: " << position << " (segment: " << segment_id << ")");

    m_position = position;
}

void SpreadWithRewiring::execute(){
    COUT_DEBUG("window start: " << m_window_start << ", window length: " << m_window_length << ", cardinality: " << m_cardinality << ", extents: " << m_window_length / m_segments_per_extent);

    // first, spread all the elements
    spread_window();

    // second, update the cardinality for each segment
    update_segment_sizes();

    // third, update the index and, in case, insert the key
    update_index();
}

size_t SpreadWithRewiring::get_segment_capacity() const {
    return m_instance.m_storage.m_segment_capacity;
}


int64_t SpreadWithRewiring::position2segment(int64_t position) const {
    auto segment_capacity = get_segment_capacity();
    int64_t segment = floor((double) position / segment_capacity );
    return segment;
}

int64_t SpreadWithRewiring::position2extent(int64_t position) const {
    auto segment_capacity = get_segment_capacity();
    int64_t segment = position2segment(position  - m_window_start * segment_capacity);
    int64_t extent = floor((double) segment / m_segments_per_extent);
    COUT_DEBUG("segment: " << segment << ", segments_per_extent: " << m_segments_per_extent << ", extent: " << extent);
    return extent;
}

int64_t SpreadWithRewiring::extent2segment(int64_t extent) const {
    return m_window_start + extent * m_segments_per_extent;
}

int64_t SpreadWithRewiring::get_current_extent() const {
    return position2extent(m_position -1);
}

/**
 * Retrieve the starting offset, in multiple of sizeof(uint64_t), for the given extent, relative to window being rebalanced
 */
size_t SpreadWithRewiring::get_offset(int64_t relative_extent_id) const {
    auto segment_capacity = get_segment_capacity();
    return static_cast<int64_t>(m_window_start * segment_capacity) + (relative_extent_id * m_segments_per_extent * segment_capacity);
}

/**
 * @param array either m_storage.m_keys or m_storage.m_values
 * @param relative_extent the extent id, relative to the window being rebalanced
 */
int64_t* SpreadWithRewiring::get_start_address(int64_t* array, int64_t relative_extent_id) const {
    return array + get_offset(relative_extent_id);
}

void SpreadWithRewiring::acquire_free_space(int64_t** space_keys, int64_t** space_values){
    *space_keys = (int64_t*) m_instance.m_storage.m_memory_keys->acquire_buffer();
    *space_values = (int64_t*) m_instance.m_storage.m_memory_values->acquire_buffer();
}

void SpreadWithRewiring::rewire_keys(int64_t* addr1, int64_t* addr2){
    m_instance.m_storage.m_memory_keys->swap_and_release(addr1, addr2);
}

void SpreadWithRewiring::rewire_values(int64_t* addr1, int64_t* addr2){
    m_instance.m_storage.m_memory_values->swap_and_release(addr1, addr2);
}

void SpreadWithRewiring::reclaim_past_extents(){
    int64_t current_extent_id = get_current_extent();
    COUT_DEBUG("current_extent_id: " << current_extent_id);
    while(!m_extents_to_rewire.empty() && m_extents_to_rewire.front().m_extent_id > current_extent_id){
        auto& metadata = m_extents_to_rewire.front();
        auto extent_id = metadata.m_extent_id;
        auto keys_dst = get_start_address(m_instance.m_storage.m_keys, extent_id);
        auto keys_src = metadata.m_buffer_keys;
        auto values_dst = get_start_address(m_instance.m_storage.m_values, extent_id);
        auto values_src = metadata.m_buffer_values;
        m_extents_to_rewire.pop_front();
        COUT_DEBUG("reclaim buffers for keys: " << keys_src << ", values: " << values_src);

        rewire_keys(keys_dst, keys_src);
        rewire_values(values_dst, values_src);
    }
}


/**
 * Spread `num_elements' elements from right to left (backwards).
 */
void SpreadWithRewiring::spread_elements(int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id, size_t num_elements){
    const int64_t elements_per_segment = num_elements / m_segments_per_extent;
    const int64_t odd_segments = num_elements % m_segments_per_extent;
    assert(elements_per_segment + 1 <= m_instance.m_storage.m_segment_capacity && "Each segment should have at least a slot free after the rebalancing");

    decltype(m_instance.m_storage.m_segment_sizes) __restrict segment_sizes = m_instance.m_storage.m_segment_sizes;
    const size_t segment_capacity = get_segment_capacity();
    int64_t input_segment_id = ((static_cast<int64_t>(m_position) -1) / (2* segment_capacity)) *2; // even segment
    int64_t input_initial_displacement = input_segment_id * segment_capacity + segment_capacity - segment_sizes[input_segment_id];
    int64_t input_run_sz = m_position - input_initial_displacement;
    COUT_DEBUG("extent: " << extent_id << ", initial segment: " << input_segment_id << ", run sz: " << input_run_sz << ", displacement: " << input_initial_displacement);
    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);
    int64_t* input_keys = m_instance.m_storage.m_keys + input_initial_displacement;
    int64_t* input_values = m_instance.m_storage.m_values + input_initial_displacement;


    for(int64_t output_segment_id = m_segments_per_extent -2; output_segment_id >= 0; output_segment_id -= 2){
        const int64_t output_run_sz_lhs = elements_per_segment + (output_segment_id < odd_segments);
        const int64_t output_run_sz_rhs = elements_per_segment + ((output_segment_id+1) < odd_segments);
        int64_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        COUT_DEBUG("output_segment_id: " << output_segment_id << ", run size: " << output_run_sz);
        assert(output_run_sz >= 0 && output_run_sz <= 2 * get_segment_capacity() -2);
        size_t output_displacement = output_segment_id * segment_capacity + (segment_capacity - output_run_sz_lhs);
        int64_t* output_keys = destination_keys + output_displacement;
        int64_t* output_values = destination_values + output_displacement;

        while(output_run_sz > 0){
            size_t elements_to_copy = min(output_run_sz, input_run_sz);
            const size_t input_copy_offset = input_run_sz - elements_to_copy;
            const size_t output_copy_offset = output_run_sz - elements_to_copy;
            memcpy(output_keys + output_copy_offset, input_keys + input_copy_offset, elements_to_copy * sizeof(output_keys[0]));
            memcpy(output_values + output_copy_offset, input_values + input_copy_offset, elements_to_copy * sizeof(output_values[0]));
            input_run_sz -= elements_to_copy;
            output_run_sz -= elements_to_copy;

            if(input_run_sz == 0){
                assert(input_segment_id % 2 == 0 && "The input segment should be always an even segment");
                input_segment_id -= 2; // move to the previous even segment
                size_t input_displacement;
                if(input_segment_id >= static_cast<int64_t>(m_window_start)){ // fetch the segment sizes
                    input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];
                    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);
                    input_displacement = input_segment_id * segment_capacity + segment_capacity - segment_sizes[input_segment_id];
                } else { // underflow
                    input_displacement = m_window_start * segment_capacity;
                }
                input_keys = m_instance.m_storage.m_keys + input_displacement;
                input_values = m_instance.m_storage.m_values + input_displacement;

                assert(input_segment_id >= static_cast<int64_t>(m_window_start) -4 && "Underflow");
            }
        }

        COUT_DEBUG("output segments: " << (extent2segment(extent_id) + output_segment_id) << " and " << (extent2segment(extent_id) + output_segment_id +1) << "; "
                "output_run_sz: " << (output_run_sz_lhs + output_run_sz_rhs) << ", first element: " << output_keys[0] << ", last element: " << output_keys[(output_run_sz_lhs + output_run_sz_rhs) -1]);
        COUT_DEBUG("input_segment_id: " << input_segment_id << ", input_run_sz: " << input_run_sz);
    }

    // update the final position
    m_position = input_keys - m_instance.m_storage.m_keys + input_run_sz;
}

void SpreadWithRewiring::spread_extent(int64_t extent_id, size_t num_elements){
    assert(extent_id >= 0 && "Underflow");
    assert(extent_id < m_window_length / m_segments_per_extent && "Overflow");
    const bool use_rewiring = get_current_extent() >= extent_id;

    if(!use_rewiring){
        COUT_DEBUG("without rewiring, extent_id: " << extent_id);
        // no need for rewiring, just spread in place as the source and destination refer to different extents
        spread_elements(get_start_address(m_instance.m_storage.m_keys, extent_id), get_start_address(m_instance.m_storage.m_values, extent_id), extent_id, num_elements);
    } else {
        // get some space from the rewiring facility
        int64_t *buffer_keys{nullptr}, *buffer_values{nullptr};
        acquire_free_space(&buffer_keys, &buffer_values);
        m_extents_to_rewire.push_back(Extent2Rewire{extent_id, buffer_keys, buffer_values});
        COUT_DEBUG("buffer_keys: " << (void*) buffer_keys << ", buffer values: " << (void*) buffer_values << ", extent: " << extent_id);
        spread_elements(buffer_keys, buffer_values, extent_id, num_elements);
    }

    reclaim_past_extents();
}

void SpreadWithRewiring::spread_window(){
    assert(m_window_length % m_segments_per_extent == 0 && "Not a multiple");
    assert(m_window_length / m_segments_per_extent > 0 && "Window too small");

    int64_t num_extents = m_window_length / m_segments_per_extent;
    int64_t elements_per_extent = m_cardinality / num_extents;
    int64_t odd_extents = m_cardinality % num_extents;

    assert(m_instance.m_storage.m_memory_keys->get_used_buffers() == 0 && "All buffers should have been released");
    assert(m_instance.m_storage.m_memory_values->get_used_buffers() == 0 && "All buffers should have been released");
    for(int64_t i = num_extents -1; i >= 0; i--){
        spread_extent(i, elements_per_extent + (i < odd_extents));
    }
    assert(m_instance.m_storage.m_memory_keys->get_used_buffers() == 0 && "All buffers should have been released");
    assert(m_instance.m_storage.m_memory_values->get_used_buffers() == 0 && "All buffers should have been released");
}

void SpreadWithRewiring::update_segment_sizes(){
    size_t num_extents = m_window_length / m_segments_per_extent;
    size_t elements_per_extent = m_cardinality / num_extents;
    size_t odd_extents = m_cardinality % num_extents;

    decltype(m_instance.m_storage.m_segment_sizes) __restrict segment_sizes = m_instance.m_storage.m_segment_sizes;
    size_t segment_id = m_window_start;

    for(size_t i = 0; i < num_extents; i++){
        size_t extent_cardinality = elements_per_extent + (i < odd_extents);

        size_t num_segments = m_segments_per_extent;
        size_t elements_per_segment = extent_cardinality / num_segments;
        size_t odd_segments = extent_cardinality % num_segments;

        for(size_t j = 0; j < num_segments; j++){
            segment_sizes[segment_id] = elements_per_segment + (j < odd_segments);
            segment_id++;
        }
    }
}

/**
 * Insert the element in the given segment_id
 * @param segment_id absolute offset for the segment, beginning from the start of the sparse array and not from m_segment_start
 */
void SpreadWithRewiring::insert(int64_t segment_id){
    assert(m_insert && "No elements to insert");
    m_instance.m_storage.insert(segment_id, m_insert_key, m_insert_value); // ignore result
    m_insert = false;
}


void SpreadWithRewiring::update_index(){
    size_t segment_id = m_window_start;
    for(size_t i = 0; i < m_window_length; i++){
        int64_t minimum = m_instance.m_storage.get_minimum(segment_id);

        if(m_insert){
            if(m_insert_key < minimum){
                if(i > 0){ // insert the element in the previous segment
                    insert(segment_id -1);
                } else {
                    minimum = m_insert_key;
                    insert(segment_id);
                }
                m_insert = false;
            }
        }

        m_instance.m_index.set_separator_key(segment_id, minimum);

        segment_id++;
    }

    if(m_insert){ // maximum
        insert(m_window_start + m_window_length -1);
        m_insert = false;
    }
}


}} // pma::v8
