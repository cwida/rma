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

#include "spread_with_rewiring.hpp"

#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

#include "buffered_rewired_memory.hpp"
#include "packed_memory_array.hpp"
#include "partition.hpp"
#include "storage.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[SpreadWithRewiring::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace pma { namespace adaptive { namespace int2 {


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

SpreadWithRewiring::SpreadWithRewiring(PackedMemoryArray* instance, size_t window_start, size_t window_length, const VectorOfPartitions& partitions)
    : m_instance(*instance), m_window_start(window_start), m_window_length(window_length),
      m_segments_per_extent(m_instance.m_storage.m_memory_keys->get_extent_size() / (m_instance.m_storage.m_segment_capacity * sizeof(uint64_t))),
      m_partitions(partitions) {
    reset_current_position();
}

/*****************************************************************************
 *                                                                           *
 *   Current position                                                        *
 *                                                                           *
 *****************************************************************************/

size_t SpreadWithRewiring::get_segment_capacity() const {
    return m_instance.m_storage.m_segment_capacity;
}

void SpreadWithRewiring::reset_current_position(){
    size_t segment_capacity = get_segment_capacity();
    m_position = m_window_start * segment_capacity + segment_capacity - m_instance.m_storage.m_segment_sizes[m_window_start];
}

int64_t SpreadWithRewiring::position2segment(int64_t position) const {
    auto segment_capacity = get_segment_capacity();
    int64_t segment = floor((double) position / segment_capacity );
    COUT_DEBUG("position: " << position << ", capacity: " << segment_capacity << ", segment: " << segment);
    return segment;
}

int64_t SpreadWithRewiring::position2extent(int64_t position) const {
    auto segment_capacity = get_segment_capacity();
    int64_t segment = position2segment(position  - m_window_start * segment_capacity);
    int64_t extent = floor((double) segment / m_segments_per_extent);
    return extent;
}

int64_t SpreadWithRewiring::extent2segment(int64_t extent) const {
    return m_window_start + extent * m_segments_per_extent;
}

int64_t SpreadWithRewiring::get_current_extent() const {
    return position2extent(static_cast<int64_t>(m_position) + (m_move_backwards ? -1 : 0));
}

size_t SpreadWithRewiring::get_offset(int64_t relative_extent_id) const {
    auto segment_capacity = get_segment_capacity();
    return static_cast<int64_t>(m_window_start * segment_capacity) + (relative_extent_id * m_segments_per_extent * segment_capacity);
}

int64_t* SpreadWithRewiring::get_start_address(int64_t* array, int64_t relative_extent_id) const {
    return array + get_offset(relative_extent_id);
}

/*****************************************************************************
 *                                                                           *
 *   Current partition                                                       *
 *                                                                           *
 *****************************************************************************/

void SpreadWithRewiring::reset_current_partition(){
    m_partition_id = m_partition_offset = 0;
}

size_t SpreadWithRewiring::get_partition_current() const{
    return get_partition_current(m_partitions, m_partition_id, m_partition_offset);
}

size_t SpreadWithRewiring::get_partition_current(const VectorOfPartitions& partitions, size_t partition_id, size_t partition_offset){
    if(partition_id >= partitions.size()){
        return 0;
    } else {
        auto partition = partitions[partition_id];
        size_t card_per_segment = partition.m_cardinality / partition.m_segments;
        size_t odd_segments = partition.m_cardinality % partition.m_segments;
        return card_per_segment + (partition_offset < odd_segments);
    }
}


size_t SpreadWithRewiring::get_partition_next() const {
    return get_partition_next(m_partitions, m_partition_id, m_partition_offset);
}

size_t SpreadWithRewiring::get_partition_next(const VectorOfPartitions& partitions_, size_t partition_id_, size_t partition_offset_){
    if(partition_id_ >= partitions_.size()) return 0;

    size_t partition_id = partition_id_;
    size_t partition_offset = partition_offset_ + 1;

    if(partition_offset >= partitions_[partition_id].m_segments){
        partition_id++;
        partition_offset = 0;

        if(partition_id >= partitions_.size()) return 0;
    }

    auto partition = partitions_[partition_id];
    size_t card_per_segment = partition.m_cardinality / partition.m_segments;
    size_t odd_segments = partition.m_cardinality % partition.m_segments;
    return card_per_segment + (partition_offset < odd_segments);
}


void SpreadWithRewiring::move_current_partition_forwards_by(size_t N){
    while(N > 0){
        if(m_partition_id >= m_partitions.size()) return; // overflow

        size_t max_step = m_partitions[m_partition_id].m_segments - m_partition_offset;
        size_t step = min(max_step, N);
        N -= step;

        if(step == max_step){ // next set of partitions
            m_partition_id++;
            m_partition_offset = 0;
        } else {
            m_partition_offset += step;
        }
    }
}

void SpreadWithRewiring::move_current_partition_backwards_by(size_t N){
//    COUT_DEBUG("N: " << N);
    while(N > 0){
        if(m_partition_id == 0 && m_partition_offset == 0) return; // underflow

        size_t step = min(N, m_partition_offset +1);
        N -= step;
        if(step > m_partition_offset){
            if(m_partition_id > 0){
                m_partition_id--;
                m_partition_offset = m_partitions[m_partition_id].m_segments -1;
            } else {
                m_partition_offset = 0;
            }
        } else {
            m_partition_offset -= step;
        }
    }
}

void SpreadWithRewiring::move_current_partition_by(int64_t N){
    if(N >= 0){ // move forwards
        move_current_partition_forwards_by(N);
    } else { // move backwards
        move_current_partition_backwards_by(-N);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Rewiring                                                                *
 *                                                                           *
 *****************************************************************************/
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
    const bool move_backwards = m_move_backwards;
    while(!m_extents_to_rewire.empty() && (
            (!move_backwards && (m_extents_to_rewire.front().m_extent_id < current_extent_id)) ||
            (move_backwards && (m_extents_to_rewire.front().m_extent_id > current_extent_id))
            )){
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

/*****************************************************************************
 *                                                                           *
 *   Spreading (general)                                                     *
 *                                                                           *
 *****************************************************************************/
template<bool move_backwards>
void SpreadWithRewiring::spread_elements(int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id){
    if(move_backwards){ // backwards
        spread_elements_right2left(destination_keys, destination_values, extent_id);
    } else { // forwards
        spread_elements_left2right(destination_keys, destination_values, extent_id);
    }
}

template<bool move_backwards>
void SpreadWithRewiring::spread_extent(int64_t extent_id){
    assert(extent_id >= 0 && "Underflow");
    assert(extent_id < m_window_length / m_segments_per_extent && "Overflow");
    const bool use_rewiring = (!move_backwards && get_current_extent() <= extent_id) || (move_backwards && get_current_extent() >= extent_id);

    if(!use_rewiring){
        COUT_DEBUG("without rewiring, extent_id: " << extent_id);
        // no need for rewiring, just spread in place as the source and destination refer to different extents
        spread_elements<move_backwards>(get_start_address(m_instance.m_storage.m_keys, extent_id), get_start_address(m_instance.m_storage.m_values, extent_id), extent_id);
    } else {
        // get some space from the rewiring facility
        int64_t *buffer_keys{nullptr}, *buffer_values{nullptr};
        acquire_free_space(&buffer_keys, &buffer_values);
        m_extents_to_rewire.push_back(Extent2Rewire{extent_id, buffer_keys, buffer_values});
        COUT_DEBUG("buffer_keys: " << (void*) buffer_keys << ", buffer values: " << (void*) buffer_values << ", extent: " << extent_id);
        spread_elements<move_backwards>(buffer_keys, buffer_values, extent_id);
    }

    reclaim_past_extents();
}

void SpreadWithRewiring::spread_window(){
    assert(m_window_length % m_segments_per_extent == 0 && "Not a multiple");
    assert(m_window_length / m_segments_per_extent > 0 && "Window too small");

    int64_t num_extents = m_window_length / m_segments_per_extent;

    assert(m_instance.m_storage.m_memory_keys->get_used_buffers() == 0 && "All buffers should have been released");
    assert(m_instance.m_storage.m_memory_values->get_used_buffers() == 0 && "All buffers should have been released");
    COUT_DEBUG("num_extents: " << num_extents << ", left to right (forwards): " << std::boolalpha << (!m_move_backwards) << std::noboolalpha);
    if(!m_move_backwards){ // forwards, left 2 right
        for(int64_t i = 0; i < num_extents; i++){
            spread_extent<false>(i);
        }
    } else { // backwards, right 2 left
        for(int64_t i = num_extents -1; i>=0; i--){
            spread_extent<true>(i);
        }
    }
    assert(m_instance.m_storage.m_memory_keys->get_used_buffers() == 0 && "All buffers should have been released");
    assert(m_instance.m_storage.m_memory_values->get_used_buffers() == 0 && "All buffers should have been released");
}


/*****************************************************************************
 *                                                                           *
 *   Spreading (left to right)                                               *
 *                                                                           *
 *****************************************************************************/
void SpreadWithRewiring::spread_elements_left2right(int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id){
    assert(!m_move_backwards && "This should method should only be invoked when rebalancing from left to right");
    COUT_DEBUG("[destination] keys: " << destination_keys << ", values: " << destination_values << ", extent_id: " << extent_id);
    decltype(m_instance.m_storage.m_segment_sizes) __restrict segment_sizes = m_instance.m_storage.m_segment_sizes;
    const size_t segment_capacity = get_segment_capacity();
    const size_t segment_base = extent2segment(extent_id);
    int64_t input_segment_id = m_position / segment_capacity;
    int64_t input_offset = m_position % segment_capacity;
    int64_t input_run_sz = 0;
    int64_t* input_keys = m_instance.m_storage.m_keys + m_position;
    int64_t* input_values = m_instance.m_storage.m_values + m_position;

    if(input_segment_id % 2 == 0){ // even segments
        input_run_sz = /* even segment */ segment_capacity - input_offset + /* odd segment */ segment_sizes[input_segment_id +1];
    } else { // odd segments
        input_run_sz = segment_sizes[input_segment_id] - input_offset;
    }
    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);

    for(size_t i = 0; i < m_segments_per_extent; i+=2){
        const int64_t output_run_sz_lhs = get_partition_current();
        assert(output_run_sz_lhs <= static_cast<int64_t>(get_segment_capacity()) && "LHS Overflow");
        const int64_t output_run_sz_rhs = get_partition_next();
        assert(output_run_sz_rhs <= static_cast<int64_t>(get_segment_capacity()) && "RHS Overflow");
        COUT_DEBUG("[" << i << "] left: " << output_run_sz_lhs << ", right: " << output_run_sz_rhs << ", partition_id: " << m_partition_id << ", partition_offset: " << m_partition_offset);
        int64_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        assert(output_run_sz >= 0 && output_run_sz <= 2 * get_segment_capacity());
        size_t output_displacement = i * segment_capacity + (segment_capacity - output_run_sz_lhs);
        int64_t* output_keys = destination_keys + output_displacement;
        int64_t* output_values = destination_values + output_displacement;

        while(output_run_sz > 0){
            size_t elements_to_copy = min(output_run_sz, input_run_sz);
            size_t input_copied;
            COUT_DEBUG("insert_segment: " << m_insert_segment << ", input_segment_id: " << input_segment_id);
            if(m_insert_segment >= 0 && m_insert_segment <= (input_segment_id +1)){ // merge manually
                COUT_DEBUG("merge key: " << m_insert_key);
                size_t output_segment_id = m_window_start + extent_id * m_segments_per_extent + i;
                int64_t output_lhs_end = static_cast<int64_t>((destination_keys + output_displacement) - output_keys) + output_run_sz_lhs;
                int64_t output_rhs_end = output_lhs_end + output_run_sz_rhs;
                merge_to_insert(output_keys, output_values, input_keys, input_values, elements_to_copy, output_segment_id, output_lhs_end, output_rhs_end);
                if(elements_to_copy == 0){ elements_to_copy = 1; }
                input_copied = elements_to_copy - (m_insert_segment == -1); // iff m_insert_segment == -1 => new element just inserted
            } else { // use memcpy
                memcpy(output_keys, input_keys, elements_to_copy * sizeof(int64_t));
                memcpy(output_values, input_values, elements_to_copy * sizeof(int64_t));
                input_copied = elements_to_copy;
            }
            input_keys += input_copied; input_values += input_copied;
            output_keys += elements_to_copy; output_values += elements_to_copy;
            input_run_sz -= input_copied;
            output_run_sz -= elements_to_copy;


            if(input_run_sz == 0){
                input_segment_id += 1 + (input_segment_id % 2 == 0); // move to the next even segment
                size_t input_displacement;
                if(input_segment_id < m_window_start + m_window_length){ // fetch the segment sizes
                    assert(input_segment_id % 2 == 0);
                    input_offset = segment_capacity - segment_sizes[input_segment_id];
                    input_run_sz = segment_sizes[input_segment_id] + segment_sizes[input_segment_id +1];
                    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);
                    input_displacement = input_segment_id * segment_capacity + input_offset;
                } else { // overflow
                    input_displacement = (m_window_start + m_window_length) * segment_capacity;
                }
                input_keys = m_instance.m_storage.m_keys + input_displacement;
                input_values = m_instance.m_storage.m_values + input_displacement;

                if(m_insert_predecessor != -1 && m_insert_successor == -1){
                    m_insert_successor = input_keys[0];
                }
            }

        }

        m_instance.m_index.set_separator_key(segment_base + i, output_keys[-output_run_sz_lhs -output_run_sz_rhs]);
        m_instance.m_index.set_separator_key(segment_base + i + 1, output_keys[-output_run_sz_rhs]);

//        /**
//         * Debug only
//         */
//        output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
//        output_keys -= output_run_sz;
//        output_values -= output_run_sz;
//        for(size_t j = 0; j < output_run_sz; j++){
//            COUT_DEBUG("[" << i << "] j: " << j << " <" << output_keys[j] << ", " << output_values[j] << ">");
//        }

        move_current_partition_by(2);
    }

    // update the final position
    m_position = input_keys - m_instance.m_storage.m_keys;
}




/*****************************************************************************
 *                                                                           *
 *   Spreading (right to left)                                               *
 *                                                                           *
 *****************************************************************************/
void SpreadWithRewiring::spread_elements_right2left(int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id){
    assert(m_move_backwards && "This should method should only be invoked when rebalancing from right to left");
    assert(m_insert_segment == -1 && "Operation not supported when rebalancing from right to left");
    COUT_DEBUG("[destination] keys: " << destination_keys << ", values: " << destination_values << ", extent_id: " << extent_id << ", position: " << m_position);
    decltype(m_instance.m_storage.m_segment_sizes) __restrict segment_sizes = m_instance.m_storage.m_segment_sizes;
    const size_t segment_capacity = get_segment_capacity();
    const size_t segment_base = extent2segment(extent_id);
    int64_t input_segment_id = ((static_cast<int64_t>(m_position) -1) / (2* segment_capacity)) *2; // even segment
    int64_t input_initial_displacement = input_segment_id * segment_capacity + segment_capacity - segment_sizes[input_segment_id];
    int64_t input_run_sz = m_position - input_initial_displacement;
    COUT_DEBUG("extent: " << extent_id << ", initial segment: " << input_segment_id << ", run sz: " << input_run_sz << ", displacement: " << input_initial_displacement);
    assert(input_run_sz > 0 && input_run_sz <= 2 * segment_capacity);
    int64_t* input_keys = m_instance.m_storage.m_keys + input_initial_displacement;
    int64_t* input_values = m_instance.m_storage.m_values + input_initial_displacement;

    for(int64_t output_segment_id_rel = m_segments_per_extent -2; output_segment_id_rel >= 0; output_segment_id_rel -= 2){ // relative to the current extent
        const int64_t output_run_sz_lhs = get_partition_current();
        assert(output_run_sz_lhs <= static_cast<int64_t>(get_segment_capacity()) && "LHS Overflow");
        const int64_t output_run_sz_rhs = get_partition_next();
        assert(output_run_sz_rhs <= static_cast<int64_t>(get_segment_capacity()) && "RHS Overflow");
        COUT_DEBUG("[e: " << extent_id << ", s: " << output_segment_id_rel << "] left: " << output_run_sz_lhs << ", right: " << output_run_sz_rhs);
        int64_t output_run_sz = output_run_sz_lhs + output_run_sz_rhs;
        assert(output_run_sz >= 0 && output_run_sz <= 2 * get_segment_capacity());
        size_t output_displacement = output_segment_id_rel * segment_capacity + (segment_capacity - output_run_sz_lhs);
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
            }
        }

        m_instance.m_index.set_separator_key(segment_base + output_segment_id_rel, output_keys[0]);
        m_instance.m_index.set_separator_key(segment_base + output_segment_id_rel + 1, output_keys[output_run_sz_lhs]);

        COUT_DEBUG("output segments: " << (segment_base + output_segment_id_rel) << " and " << (segment_base + output_segment_id_rel +1) << "; "
                "output_run_sz: " << (output_run_sz_lhs + output_run_sz_rhs) << ", first element: " << output_keys[0] << ", last element: " << output_keys[(output_run_sz_lhs + output_run_sz_rhs) -1]);
        COUT_DEBUG("input_segment_id: " << input_segment_id << ", input_run_sz: " << input_run_sz);

        move_current_partition_by(-2);
    }

    // update the final position
    m_position = input_keys - m_instance.m_storage.m_keys + input_run_sz;
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/

void SpreadWithRewiring::merge_to_insert(int64_t* __restrict output_keys, int64_t* __restrict output_values, int64_t* __restrict input_keys, int64_t* __restrict input_values,
        size_t elements_to_copy, size_t output_segment_id, int64_t output_lhs_end, int64_t output_rhs_end){
    assert(!m_move_backwards && "Operation not supported in the right to left direction");
    COUT_DEBUG("element_to_copy: " << elements_to_copy << ", key to insert: " << m_insert_key);

    int64_t insert_position = -1;
    size_t j = 0;
    while(j < elements_to_copy && input_keys[j] < m_insert_key){
        COUT_DEBUG("[" << j << "]: " << input_keys[j]);
        output_keys[j] = input_keys[j];
        output_values[j] = input_values[j];
        j++;
    }
    if(j < elements_to_copy || elements_to_copy == 0){ // iff elements_to_copy == 0 => this is the last segment in the window and the new element is the maximum
        output_keys[j] = m_insert_key;
        output_values[j] = m_insert_value;
        insert_position = j;
        j++;
    }
    assert(j > 0 && "j is size_t, if = 0 => j -1 goes in underflow. However the previous guard should have been executed, hence j > 0");

    // finish with memcpy
    if(elements_to_copy > 0){
        memcpy(output_keys + j, input_keys + j -1, (elements_to_copy - j) * sizeof(int64_t));
        memcpy(output_values + j, input_values + j -1, (elements_to_copy - j) * sizeof(int64_t));
    }

    // did we insert the element ?
    if(insert_position > -1){
        m_insert_to_segment = output_segment_id + (insert_position >= output_lhs_end);
        if((insert_position == 0 && elements_to_copy > 0) || insert_position == output_lhs_end){
            m_insert_predecessor = numeric_limits<int64_t>::min();
        } else {
            m_insert_predecessor = output_keys[insert_position -1];
        }
        if(insert_position == /* the last position in the left segment */ output_lhs_end -1 || /* last position in the right segment */ insert_position == output_rhs_end -1){
            m_insert_successor = numeric_limits<int64_t>::max();
        } else if (insert_position < elements_to_copy -1){
            m_insert_successor = output_keys[insert_position +1];
        }

        m_insert_segment = -1; // done
    }
}

void SpreadWithRewiring::finalize_insert(){
    assert(m_insert_segment == -1 && "The new element should have been inserted");
    if(m_insert_to_segment == -1) return; // there was no new element to insert
    assert(m_insert_to_segment >= m_window_start && m_insert_to_segment < m_window_start + m_window_length && "Invalid segment");
    assert(m_insert_predecessor != -1 && "Invalid value for the predecessor, it should be either >= 0 || int64_t::min");
    assert(m_insert_successor != -1 && "Invalid value for the predecessor, it should be either >= 0 || int64_t::max");
    m_instance.m_storage.m_cardinality++;
    COUT_DEBUG("key: " << m_insert_key << ", value: " << m_insert_value << ", segment: " << m_insert_to_segment << ", predecessor: " << m_insert_predecessor << ", successor: " << m_insert_successor);
    m_instance.m_detector.insert(m_insert_to_segment, m_insert_predecessor, m_insert_successor);
}

void SpreadWithRewiring::update_segment_sizes(){
    decltype(m_instance.m_storage.m_segment_sizes) __restrict segment_sizes = m_instance.m_storage.m_segment_sizes;
    size_t segment_id = m_window_start;
    size_t segment_end = m_window_start + m_window_length;
    size_t partition_id = 0;

    while(segment_id < segment_end){
        size_t partition_length = m_partitions[partition_id].m_segments;
        size_t cardinality = m_partitions[partition_id].m_cardinality;
        size_t card_per_segment = cardinality / partition_length;
        size_t odd_segments = cardinality % partition_length;

        for(size_t i = 0; i < partition_length; i++){
            assert((card_per_segment + (i < odd_segments)) <= get_segment_capacity());
            segment_sizes[segment_id] = card_per_segment + (i < odd_segments);
            segment_id++;
        }

        partition_id++;
    }
}

void SpreadWithRewiring::execute(){
    COUT_DEBUG("window start: " << m_window_start << ", window length: " << m_window_length << ", extents: " << m_window_length / m_segments_per_extent);

    spread_window();

    update_segment_sizes();

    finalize_insert();
}

void SpreadWithRewiring::set_element_to_insert(int64_t key, int64_t value, size_t segment_id){
    if(m_insert_segment != -1){
        RAISE_EXCEPTION(Exception, "An element has already been registered to be inserted: <" << m_insert_key << ", " << m_insert_value << "> @ segment: " << m_insert_segment);
    }
    if(segment_id < m_window_start || segment_id >= (m_window_start + m_window_length)){
        RAISE_EXCEPTION(Exception, "Segment ID out of the current window: " << segment_id << ", window: [" << m_window_start << ", " << m_window_start + m_window_length << ")");
    }
    if(m_move_backwards){
        RAISE_EXCEPTION(Exception, "An insert operation is not supported if the spread is from right to left");
    }

    m_insert_segment = segment_id; // starting from 0, not from the m_window_start
    m_insert_key = key;
    m_insert_value = value;
}

void SpreadWithRewiring::set_absolute_position(size_t position, bool move_backwards){
    if(m_insert_segment != -1 && move_backwards){
        RAISE_EXCEPTION(Exception, "Cannot insert an element if the spread is from right to left");
    }

    // check that position is inside the current window
    int64_t segment_id = position2segment(static_cast<int64_t>(position) + (move_backwards ? -1 : 0));
    int64_t window_start = m_window_start;
    int64_t window_end = m_window_start + m_window_length;
    if(segment_id < window_start || segment_id >= window_end){
        RAISE_EXCEPTION(Exception, "Invalid starting position: " << position << ", segment: " << segment_id << ". Invalid from the current window: [" << window_start << ", " << window_end << ").");
    }
    COUT_DEBUG("position: " << position << " (segment: " << segment_id << "), move_backwards: " << boolalpha << move_backwards << noboolalpha);

    m_position = position;
    m_move_backwards = move_backwards;

    if(move_backwards){
        m_partition_id = m_partitions.size() -1;
        m_partition_offset = m_partitions[m_partition_id].m_segments -1;
        move_current_partition_by(-1);
    } else {
        m_partition_id = m_partition_offset = 0;
    }
}

}}} // pma::adaptive::int2

