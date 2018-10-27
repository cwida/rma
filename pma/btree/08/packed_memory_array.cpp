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

#include "packed_memory_array.hpp"

#include <cmath>
#include <limits>
#include <iostream>
#include "buffered_rewired_memory.hpp"
#include "iterator.hpp"
#include "miscellaneous.hpp"
#include "rewired_memory.hpp"
#include "spread_with_rewiring.hpp"

using namespace std;

namespace pma { namespace v8 {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[PackedMemoryArray8::" << __FUNCTION__ << "] " << msg << std::endl
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
PackedMemoryArray8::PackedMemoryArray8(size_t pages_per_extent) : PackedMemoryArray8(/* B = */ 64, pages_per_extent) { }
PackedMemoryArray8::PackedMemoryArray8(size_t btree_block_size, size_t pages_per_extent) : PackedMemoryArray8(btree_block_size, btree_block_size, pages_per_extent) { }
PackedMemoryArray8::PackedMemoryArray8(size_t btree_block_size, size_t pma_segment_size, size_t pages_per_extent) :
       m_index(btree_block_size),
       m_storage(pma_segment_size, pages_per_extent),
       m_density_bounds1(0, 0.75, 0.75, 1) /* there is rationale for these hardwired thresholds */{
}

PackedMemoryArray8::~PackedMemoryArray8() {

}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/

size_t PackedMemoryArray8::size() const {
    return m_storage.m_cardinality;
}

bool PackedMemoryArray8::empty() const noexcept {
    return m_storage.m_cardinality == 0;
}

std::pair<double, double> PackedMemoryArray8::get_thresholds(int height) const {
    assert(height >= 1 && height <= m_storage.hyperheight());
    if(m_storage.m_number_segments > balanced_thresholds_cutoff()){
        return m_density_bounds1.thresholds(height);
    } else {
        return m_density_bounds0.thresholds(height);
    }
}

void PackedMemoryArray8::set_thresholds(int height_calibrator_tree){
    assert(height_calibrator_tree >= 1);
    if(m_storage.m_number_segments > balanced_thresholds_cutoff()){
        m_density_bounds1.thresholds(height_calibrator_tree, height_calibrator_tree);
    } else {
        m_density_bounds0.thresholds(height_calibrator_tree, height_calibrator_tree);
    }
}

size_t PackedMemoryArray8::balanced_thresholds_cutoff() const {
    return 64 * m_storage.get_segments_per_extent();
}

size_t PackedMemoryArray8::memory_footprint() const {
    return sizeof(PackedMemoryArray8) + m_index.memory_footprint() + m_storage.memory_footprint();
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray8::insert(int64_t key, int64_t value){
    if(UNLIKELY( empty() )){
        insert_empty(key, value);
    } else {
        size_t segment = m_index.find(key);
        insert_common(segment, key, value);
    }

//#if defined(DEBUG)
//    dump();
//#endif
}

void PackedMemoryArray8::insert_empty(int64_t key, int64_t value){
    assert(empty());
    assert(m_storage.capacity() > 0 && "The storage does not have any capacity?");

    m_index.set_separator_key(0, key);
    m_storage.m_segment_sizes[0] = 1;
    size_t pos = m_storage.m_segment_capacity -1;
    m_storage.m_keys[pos] = key;
    m_storage.m_values[pos] = value;
    m_storage.m_cardinality = 1;
}

void PackedMemoryArray8::insert_common(size_t segment_id, int64_t key, int64_t value){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_number_segments && "Overflow: attempting to access an invalid segment in the PMA");

    COUT_DEBUG("segment_id: " << segment_id << ", element: <" << key << ", " << value << ">");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        rebalance(segment_id, &key, &value);
    } else { // find a spot where to insert this element
        bool minimum_updated = m_storage.insert(segment_id, key, value);

        // have we just updated the minimum ?
        if (minimum_updated) m_index.set_separator_key(segment_id, key);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t PackedMemoryArray8::remove(int64_t key){
    if(empty()) return -1;

    auto segment_id = m_index.find(key);
    COUT_DEBUG("key: " << key << ", segment: " << segment_id);
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];
    assert(sz > 0 && "Empty segment!");

    int64_t value = -1;

    if (segment_id % 2 == 0) { // even
        size_t imin = m_storage.m_segment_capacity - sz, i;
        for(i = imin; i < m_storage.m_segment_capacity; i++){ if(keys[i] == key) break; }
        if(i < m_storage.m_segment_capacity){ // found ?
            value = values[i];
            // shift the rest of the elements by 1
            for(size_t j = i; j > imin; j--){
                keys[j] = keys[j -1];
                values[j] = values[j-1];
            }

            sz--;
            m_storage.m_segment_sizes[segment_id] = sz;
            m_storage.m_cardinality--;

            if(i == imin){ // update the pivot
                if(m_storage.m_cardinality == 0){ // global minimum
                    m_index.set_separator_key(0, numeric_limits<int64_t>::min());
                } else {
                    m_index.set_separator_key(segment_id, keys[imin +1]);
                }
            }
        } // end if (found)
    } else { // odd
        // find the key in the segment
        size_t i = 0;
        for( ; i < sz; i++){ if(keys[i] == key) break; }
        if(i < sz){ // found?
            value = values[i];
            // shift the rest of the elements by 1
            for(size_t j = i; j < sz - 1; j++){
                keys[j] = keys[j+1];
                values[j] = values[j+1];
            }

            sz--;
            m_storage.m_segment_sizes[segment_id] = sz;
            m_storage.m_cardinality--;

            // update the minimum
            if(i == 0 && sz > 0){ // sz > 0 => otherwise we are going to rebalance this segment anyway
                m_index.set_separator_key(segment_id, keys[0]);
            }
        } // end if (found)
    } // end if (odd segment)

    // shall we rebalance ?
    if(value != -1 && m_storage.m_number_segments > 1){
        // is the global density of the array less than 50% ?
        if(static_cast<double>(m_storage.m_cardinality) < 0.5 * m_storage.capacity()){
            auto plan = rebalance_plan(false, 0, 0, m_storage.m_cardinality, true);
            do_rebalance(plan);
        } else { // shal we rebalance the current segment?
            const size_t minimum_size = max<size_t>(get_thresholds(1).first * m_storage.m_segment_capacity, 1); // at least one element per segment
            if(sz < minimum_size){ rebalance(segment_id, nullptr, nullptr); }
        }
    }

//#if defined(DEBUG)
//    dump();
//#endif

    return value;
}

/*****************************************************************************
 *                                                                           *
 *   Rebalance                                                               *
 *                                                                           *
 *****************************************************************************/

void PackedMemoryArray8::rebalance(size_t segment_id, int64_t* key, int64_t* value){
    assert(((key && value) || (!key && !value)) && "Either both key & value are specified (insert) or none of them is (delete)");
    const bool is_insert = key != nullptr;

    int64_t window_start {0}, window_length {0}, cardinality {0};
    bool do_resize { false };
    rebalance_find_window(segment_id, is_insert, &window_start, &window_length, &cardinality, &do_resize);
    auto metadata = rebalance_plan(is_insert, window_start, window_length, cardinality, do_resize);

    if(is_insert){
        metadata.m_insert_key = *key;
        metadata.m_insert_value = *value;
        metadata.m_insert_segment = segment_id;
    }

    do_rebalance(metadata);
}

void PackedMemoryArray8::rebalance_find_window(size_t segment_id, bool is_insertion, int64_t* out_window_start, int64_t* out_window_length, int64_t* out_cardinality_after, bool* out_resize) const {
    assert(out_window_start != nullptr && out_window_length != nullptr && out_cardinality_after != nullptr && out_resize != nullptr);
    assert(segment_id < m_storage.m_number_segments && "Invalid segment");

    int64_t window_length = 1;
    int64_t window_id = segment_id;
    int64_t window_start = segment_id /* incl */, window_end = segment_id +1 /* excl */;
    int64_t cardinality_after = is_insertion ? m_storage.m_segment_capacity +1 : m_storage.m_segment_sizes[segment_id];
    int height = 1;
    // these inits are only valid for the edge case that the calibrator tree has height 1, i.e. the data structure contains only one segment
    double rho = 0.0, theta = 1.0, density = static_cast<double>(cardinality_after)/m_storage.m_segment_capacity;

    // determine the window to rebalance
    if(m_storage.height() > 1){
        int64_t index_left = segment_id -1;
        int64_t index_right = segment_id +1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;

            // re-align the calibrator tree
            if(window_end > m_storage.m_number_segments){
                int offset = window_end - m_storage.m_number_segments;
                window_start -= offset;
                window_end -= offset;
            }

            // find the number of elements in the interval
            while(index_left >= window_start){
                cardinality_after += m_storage.m_segment_sizes[index_left];
                index_left--;
            }
            while(index_right < window_end){
                cardinality_after += m_storage.m_segment_sizes[index_right];
                index_right++;
            }

            auto density_bounds = get_thresholds(height);
            rho = density_bounds.first;
            theta = density_bounds.second;
            density = ((double) cardinality_after) / (window_length * m_storage.m_segment_capacity);

        } while( ((is_insertion && density > theta) || (!is_insertion && density < rho))
                && height < m_storage.height());
    }

    if((is_insertion && density <= theta) || (!is_insertion && density >= rho)){ // rebalance
        *out_cardinality_after = cardinality_after;
        *out_window_start = window_start;
        *out_window_length = window_length;
        *out_resize = false;
    } else { // resize
        *out_cardinality_after = m_storage.m_cardinality + (is_insertion ? 1 : 0);
        *out_window_start = *out_window_length = 0;
        *out_resize = true;
    }
}

RebalanceMetadata PackedMemoryArray8::rebalance_plan(bool is_insert, int64_t window_start, int64_t window_length, int64_t cardinality_after, bool resize) const {
    RebalanceMetadata result;
    result.m_is_insert = is_insert;
    result.m_cardinality_after = cardinality_after;

    const size_t density_threshold = balanced_thresholds_cutoff();

    if(!resize){
        result.m_operation = RebalanceOperation::REBALANCE;
        result.m_window_start = window_start;
        result.m_window_length = window_length;
    } else if (is_insert){ // resize on insertion
        result.m_window_start = 0;
        size_t ideal_number_of_segments = max<size_t>(ceil( static_cast<double>(cardinality_after) / (m_density_bounds1.get_upper_threshold_root() * m_storage.m_segment_capacity) ), m_storage.m_number_segments +1);
        if(ideal_number_of_segments < density_threshold){
            result.m_window_length = m_storage.m_number_segments * 2;
            if(result.m_window_length > m_storage.get_segments_per_extent()){ // use rewiring
                result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
            } else {
                result.m_operation = RebalanceOperation::RESIZE;
            }
        } else {
            result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
            const size_t segments_per_extent = m_storage.get_segments_per_extent();
            size_t num_extents = ceil( static_cast<double>(ideal_number_of_segments) / segments_per_extent );
            assert(num_extents >= m_storage.get_number_extents());
            if(num_extents == m_storage.get_number_extents()) num_extents++;
            result.m_window_length = num_extents * segments_per_extent;
        }
    } else { // resize on deletion
        result.m_window_start = 0;
        const size_t ideal_number_of_segments = floor( static_cast<double>(cardinality_after) / static_cast<double>(m_density_bounds1.get_upper_threshold_root() * m_storage.m_segment_capacity));
        const size_t segments_per_extent = m_storage.get_segments_per_extent();
        size_t num_extents = floor( static_cast<double>(ideal_number_of_segments) / segments_per_extent );
        assert(num_extents <= m_storage.get_number_extents());
        if(num_extents == m_storage.get_number_extents()) num_extents--;

        if(num_extents * segments_per_extent <= density_threshold){
            if(m_storage.m_number_segments > density_threshold){
                result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
                result.m_window_length = density_threshold;
            } else {
                result.m_operation = RebalanceOperation::RESIZE;
                result.m_window_length = m_storage.m_number_segments /2;
            }
        } else {
            result.m_window_length = num_extents * segments_per_extent;


            if(m_storage.m_memory_keys == nullptr) {
                result.m_operation = RebalanceOperation::RESIZE;
            } else if((m_storage.m_memory_keys->get_allocated_extents() - num_extents) + m_storage.m_memory_keys->get_total_buffers() >= 2 * num_extents){
                // we wasting too much space from the rewiring facility ?
                result.m_operation = RebalanceOperation::RESIZE;
            } else {
                result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
            }
        }
    }

    return result;
}

void PackedMemoryArray8::do_rebalance(const RebalanceMetadata& action) {



    switch(action.m_operation){
    case RebalanceOperation::REBALANCE: {
        if (action.m_window_length < m_storage.get_segments_per_extent()){
            spread_local(action); // local to the extent
        } else { // use rewiring
//            COUT_DEBUG_FORCE("REBALANCE w/REWIRING, cardinality: " << action.get_cardinality_after() << ", window: [" << action.m_window_start << ", " << action.m_window_start + action.m_window_length << ")");
            SpreadWithRewiring instance{ this, (size_t) action.m_window_start, (size_t) action.m_window_length, (size_t) action.get_cardinality_before() };
            if(action.m_is_insert){ instance.set_element_to_insert(action.m_insert_key, action.m_insert_value); }
            instance.execute();
        }
    } break;
    case RebalanceOperation::RESIZE_REBALANCE: { // use rewiring
//        COUT_DEBUG_FORCE("REBALANCE RESIZE: cardinality: " << action.get_cardinality_after() << ", " << m_storage.m_number_segments << " -> " << action.m_window_length);
        resize_rebalance(action);
        set_thresholds(m_storage.hyperheight());
    } break;
    case RebalanceOperation::RESIZE: {
        resize(action);
        set_thresholds(m_storage.hyperheight());
    } break;
    default:
        assert(0 && "Invalid operation");
    }
}

/*****************************************************************************
 *                                                                           *
 *   Full resize                                                             *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray8::resize(const RebalanceMetadata& action) {
    bool do_insert = action.m_is_insert;
    size_t num_segments = action.m_window_length; // new number of segments
    size_t elements_per_segment = m_storage.m_cardinality / num_segments;
    size_t odd_segments = m_storage.m_cardinality % num_segments;
    COUT_DEBUG("# segments, from: " << m_storage.m_number_segments << " -> " << num_segments);

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_sizes) ixSizes;
    BufferedRewiredMemory* ixRewiredMemoryKeys;
    BufferedRewiredMemory* ixRewiredMemoryValues;
    RewiredMemory* ixRewiredMemoryCardinalities;
    m_storage.alloc_workspace(num_segments, &ixKeys, &ixValues, &ixSizes, &ixRewiredMemoryKeys, &ixRewiredMemoryValues, &ixRewiredMemoryCardinalities);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_sizes);
    swap(ixRewiredMemoryKeys, m_storage.m_memory_keys);
    swap(ixRewiredMemoryValues, m_storage.m_memory_values);
    swap(ixRewiredMemoryCardinalities, m_storage.m_memory_sizes);
    auto xDeleter = [&](void*){ Storage::dealloc_workspace(&ixKeys, &ixValues, &ixSizes, &ixRewiredMemoryKeys, &ixRewiredMemoryValues, &ixRewiredMemoryCardinalities); };
    unique_ptr<PackedMemoryArray8, decltype(xDeleter)> ixCleanup { this, xDeleter };
    int64_t* __restrict xKeys = m_storage.m_keys;
    int64_t* __restrict xValues = m_storage.m_values;
    decltype(m_storage.m_segment_sizes) __restrict xSizes = m_storage.m_segment_sizes;

    m_index.rebuild(num_segments);

    // fetch the first non-empty input segment
    size_t input_segment_id = 0;
    size_t input_size = ixSizes[0];
    int64_t* input_keys = ixKeys + m_storage.m_segment_capacity;
    int64_t* input_values = ixValues + m_storage.m_segment_capacity;
    bool input_segment_odd = false; // consider '0' as even
    if(input_size == 0){ // corner case, the first segment is empty!
        assert(!do_insert && "Otherwise we shouldn't see empty segments");
        input_segment_id = 1;
        input_segment_odd = true; // segment '1' is odd
        input_size = ixSizes[1];
    } else { // stick to the first segment, even!
        input_keys -= input_size;
        input_values -= input_size;
    }

    // start copying the elements
    bool output_segment_odd = false; // consider '0' as even
    for(size_t j = 0; j < num_segments; j++){
        // copy `elements_per_segment' elements at the start
        size_t elements_to_copy = elements_per_segment;
        if ( j < odd_segments ) elements_to_copy++;
        COUT_DEBUG("j: " << j << ", elements_to_copy: " << elements_to_copy);

        size_t output_offset = output_segment_odd ? 0 : m_storage.m_segment_capacity - elements_to_copy;
        size_t output_canonical_index = j * m_storage.m_segment_capacity;
        int64_t* output_keys = xKeys + output_canonical_index + output_offset;
        int64_t* output_values = xValues + output_canonical_index + output_offset;
        xSizes[j] = elements_to_copy;
        m_index.set_separator_key(j, input_keys[0]);

        do {
            assert(elements_to_copy <= m_storage.m_segment_capacity && "Overflow");

            size_t cpy1 = min(elements_to_copy, input_size);
            memcpy(output_keys, input_keys, cpy1 * sizeof(m_storage.m_keys[0]));
            output_keys += cpy1; input_keys += cpy1;
            memcpy(output_values, input_values, cpy1 * sizeof(m_storage.m_values[0]));
            output_values += cpy1; input_values += cpy1;
            input_size -= cpy1;
            COUT_DEBUG("cpy1: " << cpy1 << ", elements_to_copy: " << elements_to_copy - cpy1 << ", input_size: " << input_size);

            if(input_size == 0){ // move to the next input segment
                input_segment_id++;
                input_segment_odd = !input_segment_odd;

                if(input_segment_id < m_storage.m_number_segments){ // avoid overflows
                    input_size = ixSizes[input_segment_id];

                    // in case of ::remove(), we might find an empty segment, skip it!
                    if(input_size == 0){
                        assert(!do_insert && "Otherwise we shouldn't see empty segments");
                        input_segment_id++;
                        input_segment_odd = !input_segment_odd; // flip again
                        if(input_segment_id < m_storage.m_number_segments){
                            input_size = ixSizes[input_segment_id];
                            assert(input_size > 0 && "Only a single empty segment should exist...");
                        }
                    }

                    size_t offset = input_segment_odd ? 0 : m_storage.m_segment_capacity - input_size;
                    size_t input_canonical_index = input_segment_id * m_storage.m_segment_capacity;
                    input_keys = ixKeys + input_canonical_index + offset;
                    input_values = ixValues + input_canonical_index + offset;
                }
                assert(input_segment_id <= (m_storage.m_number_segments +1) && "Infinite loop");
            }

            elements_to_copy -= cpy1;
        } while(elements_to_copy > 0);

        // should we insert a new element in this bucket
        if(do_insert && action.m_insert_key < output_keys[-1]){
            auto min = m_storage.insert(j, action.m_insert_key, action.m_insert_value);
            if(min) m_index.set_separator_key(j, action.m_insert_key); // update the minimum in the B+ tree
            do_insert = false;
        }

        output_segment_odd = !output_segment_odd; // flip
    }

    // if the element hasn't been inserted yet, it means it has to be placed in the last segment
    if(do_insert){
        auto min = m_storage.insert(num_segments -1, action.m_insert_key, action.m_insert_value);
        if(min) m_index.set_separator_key(num_segments -1, action.m_insert_key); // update the minimum in the B+ tree
        do_insert = false;
    }

    // update the PMA properties
    m_storage.m_number_segments = num_segments;
}

/*****************************************************************************
 *                                                                           *
 *   Resize using rewiring                                                   *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray8::resize_rebalance(const RebalanceMetadata& action) {
    const size_t num_segments_before = m_storage.m_number_segments;
    const size_t num_segments_after = action.m_window_length;
    COUT_DEBUG("segments: " << num_segments_before << " -> " << num_segments_after);

    // 1) Extend the PMA
    if(num_segments_after > num_segments_before){ m_storage.extend(num_segments_after - num_segments_before); }
    m_index.rebuild(num_segments_after);

    // 2) Spread
    SpreadWithRewiring rewiring_instance(this, 0, num_segments_after, m_storage.m_cardinality /* FIXME +1 ? */ );
    if(action.m_is_insert){ rewiring_instance.set_element_to_insert(action.m_insert_key, action.m_insert_value); }
    size_t start_position = (num_segments_before -1) * m_storage.m_segment_capacity + m_storage.m_segment_sizes[num_segments_before -1];
    rewiring_instance.set_start_position(start_position);
    rewiring_instance.execute();

    // 3) Shrink the PMA
    if(num_segments_after < num_segments_before){ m_storage.shrink(num_segments_before - num_segments_after); }
}


/*****************************************************************************
 *                                                                           *
 *   Spread without rewiring                                                 *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray8::spread_local(const RebalanceMetadata& action){
    assert((action.m_is_insert || action.m_insert_segment == -1) && "In case of deletions, the insert segment should be set to -1");
    int64_t insert_segment_id = action.m_insert_segment - action.m_window_start;
    COUT_DEBUG("size: " << action.get_cardinality_after() << ", start: " << action.m_window_start << ", length: " << action.m_window_length << ", insertion segment: " << insert_segment_id);
    assert(action.m_window_start % 2 == 0 && "Expected to start from an even segment");
    assert(action.m_window_length % 2 == 0 && "Expected an even number of segments");

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    segment_size_t* __restrict sizes = m_storage.m_segment_sizes + action.m_window_start;
    int64_t* __restrict output_keys = m_storage.m_keys + action.m_window_start * m_storage.m_segment_capacity;
    int64_t* __restrict output_values = m_storage.m_values + action.m_window_start * m_storage.m_segment_capacity;

    // input chunk 2 (extra space)
    const size_t input_chunk2_capacity = static_cast<size_t>(m_storage.m_segment_capacity) *4 +1;
    size_t input_chunk2_size = 0;
    auto& memory_pool = m_memory_pool;
    auto memory_pool_deleter = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_chunk2_keys_ptr { m_memory_pool.allocate<int64_t>(input_chunk2_capacity), memory_pool_deleter };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_chunk2_values_ptr {  m_memory_pool.allocate<int64_t>(input_chunk2_capacity), memory_pool_deleter };
    int64_t* __restrict input_chunk2_keys = input_chunk2_keys_ptr.get();
    int64_t* __restrict input_chunk2_values = input_chunk2_values_ptr.get();

    // input chunk1 (it overlaps the current window)
    int64_t* __restrict input_chunk1_keys = nullptr;
    int64_t* __restrict input_chunk1_values = nullptr;
    size_t input_chunk1_size = 0;

    { // 1) first, compact all elements towards the end
        int64_t output_segment_id = action.m_window_length -2;
        int64_t output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
        int64_t output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];

        // copy the last four segments into input_chunk2_capacity
        int input_chunk2_segments_copied = 0;
        size_t input_chunk2_space_left = input_chunk2_capacity;
        while(output_segment_id >= 0 && input_chunk2_segments_copied < 4){
            size_t elements2copy = output_end - output_start;
            COUT_DEBUG("input_chunk2_segments_copied: " << input_chunk2_segments_copied << ", input_chunk2_space_left: " << input_chunk2_space_left << ", output_segment_id: " << output_segment_id << ", elements2copy: " << elements2copy);
            if(insert_segment_id == output_segment_id || insert_segment_id == output_segment_id +1){
                spread_insert_unsafe(output_keys + output_start, output_values + output_start,
                        input_chunk2_keys + input_chunk2_space_left - elements2copy -1, input_chunk2_values + input_chunk2_space_left - elements2copy -1,
                        elements2copy, action.m_insert_key, action.m_insert_value);
                input_chunk2_space_left--;
            } else {
                memcpy(input_chunk2_keys + input_chunk2_space_left - elements2copy, output_keys + output_start, elements2copy * sizeof(input_chunk2_keys[0]));
                memcpy(input_chunk2_values + input_chunk2_space_left - elements2copy, output_values + output_start, elements2copy * sizeof(input_chunk2_values[0]));
            }
            input_chunk2_space_left -= elements2copy;

            // fetch the next chunk
            output_segment_id -= 2;
            if(output_segment_id >= 0){
                output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
                output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
            }

            input_chunk2_segments_copied += 2;
        }

        // readjust the pointers for input_chunk2
        input_chunk2_keys += input_chunk2_space_left;
        input_chunk2_values += input_chunk2_space_left;
        input_chunk2_size = input_chunk2_capacity - input_chunk2_space_left;

        // move the remaining elements towards the end of the array
        int64_t input_chunk1_current = action.m_window_length * m_storage.m_segment_capacity;
        while(output_segment_id >= 0){
            size_t elements2copy = output_end - output_start;
            if(insert_segment_id == output_segment_id || insert_segment_id == output_segment_id +1){
                spread_insert_unsafe(output_keys + output_start, output_values + output_start,
                        output_keys + input_chunk1_current - elements2copy -1, output_values + input_chunk1_current - elements2copy -1,
                        elements2copy, action.m_insert_key, action.m_insert_value);
                input_chunk1_current--;
            } else {
                memcpy(output_keys + input_chunk1_current - elements2copy, output_keys + output_start, elements2copy * sizeof(output_keys[0]));
                memcpy(output_values + input_chunk1_current - elements2copy, output_values + output_start, elements2copy * sizeof(output_values[0]));
            }
            input_chunk1_current -= elements2copy;

            // fetch the next chunk
            output_segment_id -= 2;
            if(output_segment_id >= 0){
                output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
                output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
            }
        }

        // readjust the pointers for input_chunk1
        input_chunk1_size = action.m_window_length * m_storage.m_segment_capacity - input_chunk1_current;
        input_chunk1_keys = output_keys + input_chunk1_current;
        input_chunk1_values = output_values + input_chunk1_current;
    }

    // 2) set the expected size of each segment
    const size_t elements_per_segment = action.get_cardinality_after() / action.m_window_length;
    const size_t num_odd_segments = action.get_cardinality_after() % action.m_window_length;
    for(size_t i = 0; i < action.m_window_length; i++){
        sizes[i] = elements_per_segment + (i < num_odd_segments);
    }

    // 3) initialise the input chunk
    int64_t* __restrict input_keys;
    int64_t* __restrict input_values;
    size_t input_current = 0;
    size_t input_size;
    if(input_chunk1_size > 0){
        input_keys = input_chunk1_keys;
        input_values = input_chunk1_values;
        input_size = input_chunk1_size;
    } else {
        input_keys = input_chunk2_keys;
        input_values = input_chunk2_values;
        input_size = input_chunk2_size;
    }

    // 4) copy from the input chunks
    COUT_DEBUG("cardinality: " << action.get_cardinality_after() << ", chunk1 size: " << input_chunk1_size << ", chunk2 size: " << input_chunk2_size);
    for(size_t i = 0; i < action.m_window_length; i+=2){
        const size_t output_start = (i +1) * m_storage.m_segment_capacity - sizes[i];
        const size_t output_end = output_start + sizes[i] + sizes[i+1];
        size_t output_current = output_start;

        COUT_DEBUG("segments: [" << i << ", " << i+1 << "], required elements: " << output_end - output_start);

        while(output_current < output_end){
            size_t elements2copy = min(output_end - output_current, input_size - input_current);
            COUT_DEBUG("elements2copy: " << elements2copy << " output_start: " << output_start << ", output_end: " << output_end << ", output_current: " << output_current);
            memcpy(output_keys + output_current, input_keys + input_current, elements2copy * sizeof(output_keys[0]));
            memcpy(output_values + output_current, input_values + input_current, elements2copy * sizeof(output_values[0]));
            output_current += elements2copy;
            input_current += elements2copy;
            // switch to the second chunk
            if(input_current == input_size && input_keys == input_chunk1_keys){
                input_keys = input_chunk2_keys;
                input_values = input_chunk2_values;
                input_size = input_chunk2_size;
                input_current = 0;
            }
        }

        // update the separator keys
        m_index.set_separator_key(action.m_window_start + i, output_keys[output_start]);
        m_index.set_separator_key(action.m_window_start + i + 1, output_keys[output_start + sizes[i]]);
    }
}

void PackedMemoryArray8::spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value){
    size_t i = 0;
    while(i < num_elements && keys_from[i] < new_key){
        keys_to[i] = keys_from[i];
        values_to[i] = values_from[i];
        i++;
    }
    keys_to[i] = new_key;
    values_to[i] = new_value;

    memcpy(keys_to + i + 1, keys_from + i, (num_elements -i) * sizeof(keys_to[0]));
    memcpy(values_to + i + 1, values_from + i, (num_elements -i) * sizeof(values_to[0]));

    m_storage.m_cardinality++;
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/
int64_t PackedMemoryArray8::find(int64_t key) const {
    if(empty()) return -1;

    auto segment_id = m_index.find(key);
//    COUT_DEBUG("key: " << key << ", bucket: " << segment_id);
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];

    size_t start, stop;

    if(segment_id % 2 == 0){ // even
        stop = m_storage.m_segment_capacity;
        start = stop - sz;
    } else { // odd
        start = 0;
        stop = sz;
    }

    for(size_t i = start; i < stop; i++){
        if(keys[i] == key){
            return *(m_storage.m_values + segment_id * m_storage.m_segment_capacity + i);
        }
    }

    return -1;
}

/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/
unique_ptr<pma::Iterator> PackedMemoryArray8::empty_iterator() const{
    return make_unique<pma::v8::Iterator>(m_storage);
}

unique_ptr<pma::Iterator> PackedMemoryArray8::iterator() const {
    if(empty()) return empty_iterator();
    return make_unique<pma::v8::Iterator> (m_storage, 0, m_storage.m_number_segments -1,
            numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max()
    );
}

/*****************************************************************************
 *                                                                           *
 *   Aggregate sum                                                           *
 *                                                                           *
 *****************************************************************************/
pma::Interface::SumResult PackedMemoryArray8::sum(int64_t min, int64_t max) const {
    if((min > max) || empty()){ return SumResult{}; }
    int64_t segment_start = m_index.find_first(min);
    int64_t segment_end = m_index.find_last(max);
    if(segment_end < segment_start){ return SumResult{}; }

    int64_t* __restrict keys = m_storage.m_keys;

    bool notfound = true;
    ssize_t segment_id = segment_start;
    bool segment_even = segment_id % 2 == 0;
    ssize_t start = -1, stop = -1, offset = -1;

    // start of the interval
    while(notfound && segment_id < m_storage.m_number_segments){
        if(segment_even){
            stop = (segment_id +1) * m_storage.m_segment_capacity;
            start = stop - m_storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, even segment, start: " << start << ", stop: " << stop);
        } else { // odd
            start = segment_id * m_storage.m_segment_capacity;
            stop = start + m_storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, odd segment, start: " << start << ", stop: " << stop);
        }
        offset = start;

        while(offset < stop && keys[offset] < min) {
            COUT_DEBUG("lower interval, offset: " << offset << ", key: " << keys[offset] << ", key_min: " << min);
            offset++;
        }

        notfound = (offset == stop);
        if(notfound){
            segment_id++;
            segment_even = !segment_even; // flip
        }
    }

    if(segment_even && segment_id < (m_storage.m_number_segments -1)){
        stop = (segment_id +1) * m_storage.m_segment_capacity + m_storage.m_segment_sizes[segment_id +1]; // +1 implicit
    }

    if(notfound || keys[offset] > max){ return SumResult{}; }

    ssize_t end;
    { // find the last qualifying index
        assert(segment_end < m_storage.m_number_segments);
        auto interval_start_segment = segment_id;
        ssize_t segment_id = segment_end;
        bool segment_even = segment_id % 2 == 0;
        notfound = true;
        ssize_t offset, start, stop;

        while(notfound && segment_id >= interval_start_segment){
            if(segment_even){
                start = (segment_id +1) * m_storage.m_segment_capacity -1;
                stop = start - m_storage.m_segment_sizes[segment_id];
            } else { // odd
                stop = segment_id * m_storage.m_segment_capacity;
                start = stop + m_storage.m_segment_sizes[segment_id] -1;
            }
            COUT_DEBUG("upper interval, " << (segment_even ? "even":"odd") << " segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
            offset = start;

            while(offset >= stop && keys[offset] > max){
                COUT_DEBUG("upper interval, offset: " << offset << ", key: " << keys[offset] << ", key_max: " << max);
                offset--;
            }

            notfound = offset < stop;
            if(notfound){
                segment_id--;
                segment_even = !segment_even; // flip
            }
        }

        end = offset +1;
    }

    if(end <= offset) return SumResult{};
    stop = std::min(stop, end);

    int64_t* __restrict values = m_storage.m_values;
    SumResult sum;
    sum.m_first_key = keys[offset];

    while(offset < end){
        sum.m_num_elements += (stop - offset);
        while(offset < stop){
            sum.m_sum_keys += keys[offset];
            sum.m_sum_values += values[offset];
            offset++;
        }

        segment_id += 1 + (segment_id % 2 == 0); // next even segment
        if(segment_id < m_storage.m_number_segments){
            ssize_t size_lhs = m_storage.m_segment_sizes[segment_id];
            ssize_t size_rhs = m_storage.m_segment_sizes[segment_id +1];
            offset = (segment_id +1) * m_storage.m_segment_capacity - size_lhs;
            stop = std::min(end, offset + size_lhs + size_rhs);
        }
    }
    sum.m_last_key = keys[end -1];

    return sum;
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
std::ostream& operator<<(std::ostream& out, const PackedMemoryArray8& pma){
    pma.dump(out);
    return out;
}

void PackedMemoryArray8::dump() const {
    dump(cout);
}

void PackedMemoryArray8::dump(std::ostream& out) const {
    bool integrity_check = true;

    m_index.dump(out, &integrity_check);

    out << "\n";

    dump_storage(out, &integrity_check);

    assert(integrity_check && "Integrity check failed!");
}

void PackedMemoryArray8::dump_storage(std::ostream& out, bool* integrity_check) const {
    cout << "[PMA] cardinality: " << m_storage.m_cardinality << ", capacity: " << m_storage.capacity() << ", " <<
            "height: "<< m_storage.hyperheight() << ", #segments: " << m_storage.m_number_segments <<
            ", blksz #elements: " << m_storage.m_segment_capacity << ", pages per extent: " << m_storage.m_pages_per_extent <<
            ", # segments for balanced thresholds: " << balanced_thresholds_cutoff() << endl;

    if(empty()){ // edge case
        cout << "-- empty --" << endl;
        return;
    }

    int64_t previous_key = numeric_limits<int64_t>::min();

    int64_t* keys = m_storage.m_keys;
    int64_t* values = m_storage.m_values;
    auto sizes = m_storage.m_segment_sizes;
    size_t tot_count = 0;

    for(size_t i = 0; i < m_storage.m_number_segments; i++){
        out << "[" << i << "] ";

        tot_count += sizes[i];
        bool even = i % 2 == 0;
        size_t start = even ? m_storage.m_segment_capacity - sizes[i] : 0;
        size_t end = even ? m_storage.m_segment_capacity : sizes[i];

        for(size_t j = start, sz = end; j < sz; j++){
            if(j > start) out << ", ";
            out << "<" << keys[j] << ", " << values[j] << ">";

            // sanity check
            if(keys[j] < previous_key){
                out << " (ERROR: order mismatch: " << previous_key << " > " << keys[j] << ")";
                if(integrity_check) *integrity_check = false;
            }
            previous_key = keys[j];
        }
        out << endl;

        if(keys[start] != m_index.get_separator_key(i)){
            out << " (ERROR: invalid pivot, minimum: " << keys[start] << ", pivot: " << m_index.get_separator_key(i) <<  ")" << endl;
            if(integrity_check) *integrity_check = false;
        }

        // next segment
        keys += m_storage.m_segment_capacity;
        values += m_storage.m_segment_capacity;
    }

    if(m_storage.m_cardinality != tot_count){
        out << " (ERROR: size mismatch, pma registered cardinality: " << m_storage.m_cardinality << ", computed cardinality: " << tot_count <<  ")" << endl;
        if(integrity_check) *integrity_check = false;
    }
}

}} // pma::v8
