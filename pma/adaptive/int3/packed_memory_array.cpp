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

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <type_traits>
#include <vector>

#include "adaptive_rebalancing.hpp"
#include "buffered_rewired_memory.hpp"
#include "configuration.hpp"
#include "database.hpp"
#include "iterator.hpp"
#include "miscellaneous.hpp"
#include "move_detector_info.hpp"
#include "rewired_memory.hpp"
#include "spread_with_rewiring.hpp"
#include "sum.hpp"
#include "weights.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int3 {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[PackedMemoryArray::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialization                                                          *
 *                                                                           *
 *****************************************************************************/
PackedMemoryArray::PackedMemoryArray(size_t pages_per_extent) : PackedMemoryArray(/* B = */ 64, pages_per_extent) { }
PackedMemoryArray::PackedMemoryArray(size_t btree_block_size, size_t pages_per_extent) : PackedMemoryArray(btree_block_size, btree_block_size, pages_per_extent) { }
PackedMemoryArray::PackedMemoryArray(size_t btree_block_size, size_t pma_segment_size, size_t pages_per_extent) :
       m_index(btree_block_size),
       m_storage(pma_segment_size, pages_per_extent),
       m_detector(m_knobs, 1, 8),
       m_density_bounds1(0, 0.75, 0.75, 1) /* there is rationale for these hardwired thresholds */{
}

PackedMemoryArray::~PackedMemoryArray() {
    if(m_segment_statistics) record_segment_statistics();

//#if defined(PROFILING)
//    if(config().db() != nullptr)
//        m_rebalancing_profiler.save_results();
//#endif
}

/*****************************************************************************
 *                                                                           *
 *   Properties                                                              *
 *                                                                           *
 *****************************************************************************/
size_t PackedMemoryArray::size() const noexcept {
    return m_storage.m_cardinality;
}

bool PackedMemoryArray::empty() const noexcept {
    return m_storage.m_cardinality == 0;
}

const CachedDensityBounds& PackedMemoryArray::get_thresholds() const {
    if(m_primary_densities){
        return m_density_bounds1;
    } else {
        return m_density_bounds0;
    }
}

std::pair<double, double> PackedMemoryArray::get_thresholds(int height) const {
    assert(height >= 1 && height <= m_storage.hyperheight());
    return get_thresholds().thresholds(height);
}

void PackedMemoryArray::set_thresholds(int height_calibrator_tree){
    assert(height_calibrator_tree >= 1);
    if(m_primary_densities){
        m_density_bounds1.thresholds(height_calibrator_tree, height_calibrator_tree);
    } else {
        m_density_bounds0.thresholds(height_calibrator_tree, height_calibrator_tree);
    }
}

void PackedMemoryArray::set_thresholds(const RebalanceMetadata& action){
    if(action.m_operation == RebalanceOperation::RESIZE || action.m_operation == RebalanceOperation::RESIZE_REBALANCE){
        m_primary_densities = action.m_window_length > balanced_thresholds_cutoff();
        set_thresholds(ceil(log2(action.m_window_length)) +1);
    }
}

size_t PackedMemoryArray::balanced_thresholds_cutoff() const {
    return 64 * m_storage.get_segments_per_extent();
}

CachedMemoryPool& PackedMemoryArray::memory_pool() {
    return m_memory_pool;
}

Detector& PackedMemoryArray::detector(){
    return m_detector;
}

Knobs& PackedMemoryArray::knobs(){
    return m_knobs;
}

size_t PackedMemoryArray::get_segment_capacity() const noexcept{
    return m_storage.m_segment_capacity;
}

size_t PackedMemoryArray::memory_footprint() const {
    size_t space_index = m_index.memory_footprint();
    size_t space_storage = m_storage.memory_footprint();
    size_t space_detector = m_detector.capacity() * m_detector.sizeof_entry() * sizeof(uint64_t);

    return sizeof(decltype(*this)) + space_index + space_storage + space_detector;
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/

void PackedMemoryArray::insert(int64_t key, int64_t value){
    COUT_DEBUG("element: <" << key << ", " << value << ">");

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

void PackedMemoryArray::insert_empty(int64_t key, int64_t value){
    assert(empty());
    assert(m_storage.capacity() > 0 && "The storage does not have any capacity?");

    m_index.set_separator_key(0, key);
    m_storage.m_segment_sizes[0] = 1;
    size_t pos = m_storage.m_segment_capacity -1;
    m_storage.m_keys[pos] = key;
    m_storage.m_values[pos] = value;
    m_storage.m_cardinality = 1;
}

void PackedMemoryArray::insert_common(size_t segment_id, int64_t key, int64_t value){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_number_segments && "Overflow: attempting to access an invalid segment in the PMA");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        rebalance(segment_id, &key, &value);
    } else { // find a spot where to insert this element
        int64_t predecessor, successor;
        bool minimum_updated = m_storage.insert(segment_id, key, value, &predecessor, &successor);
        m_detector.insert(segment_id, predecessor, successor);

        // have we just updated the minimum ?
        if (minimum_updated) m_index.set_separator_key(segment_id, key);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t PackedMemoryArray::remove(int64_t key){
    if(empty()) return -1;

    auto segment_id = m_index.find(key);
    COUT_DEBUG("key: " << key << ", segment: " << segment_id);
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];
    assert(sz > 0 && "Empty segment!");

    int64_t value = -1;
    int64_t predecessor = numeric_limits<int64_t>::min(); // to forward to the detector/predictor
    int64_t successor = numeric_limits<int64_t>::max(); // to forward to the detector/predictor

    if (segment_id % 2 == 0) { // even
        size_t imin = m_storage.m_segment_capacity - sz, i;
        for(i = imin; i < m_storage.m_segment_capacity; i++){ if(keys[i] == key) break; }
        if(i < m_storage.m_segment_capacity){ // found ?
            // to update the predictor/detector
            if(i > imin) predecessor = keys[i-1];
            if(i < m_storage.m_segment_capacity -1) successor = keys[i+1];

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
            // to update the predictor/detector
            if(i > 0) predecessor = keys[i-1];
            if(i < sz -1) successor = keys[i+1];

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
    if(value != -1){
        m_detector.remove(segment_id, predecessor, successor);

        if(m_storage.m_number_segments >= 2 * balanced_thresholds_cutoff() && static_cast<double>(m_storage.m_cardinality) < 0.5 * m_storage.capacity()){
            auto plan = rebalance_plan(false, 0, 0, m_storage.m_cardinality, true);
            rebalance_run_apma(plan);
            do_rebalance(plan);
        } else if(m_storage.m_number_segments > 1) {
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

void PackedMemoryArray::rebalance(size_t segment_id, int64_t* key, int64_t* value){
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

    rebalance_run_apma(metadata);
    do_rebalance(metadata);
}

void PackedMemoryArray::rebalance_find_window(size_t segment_id, bool is_insertion, int64_t* out_window_start, int64_t* out_window_length, int64_t* out_cardinality_after, bool* out_resize) const {
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

            auto density_bounds = get_thresholds(height);
            rho = density_bounds.first;
            theta = density_bounds.second;

            // find the number of elements in the interval
            while(index_left >= window_start){
                cardinality_after += m_storage.m_segment_sizes[index_left];
                index_left--;
            }
            while(index_right < window_end){
                cardinality_after += m_storage.m_segment_sizes[index_right];
                index_right++;
            }

            density = ((double) cardinality_after) / (window_length * m_storage.m_segment_capacity);

        } while( ((is_insertion && density > theta) || (!is_insertion && density < rho))
                && height < m_storage.height());
    }

    COUT_DEBUG("rho: " << rho << ", density: " << density << ", theta: " << theta << ", height: " << height << ", calibrator tree: " << m_storage.height() << ", is_insert: " << is_insertion);
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

RebalanceMetadata PackedMemoryArray::rebalance_plan(bool is_insert, int64_t window_start, int64_t window_length, int64_t cardinality_after, bool resize) const {
    RebalanceMetadata result { const_cast<CachedMemoryPool&>( m_memory_pool ) };
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
                // we're wasting too much space from the rewiring facility ?
                result.m_operation = RebalanceOperation::RESIZE;
            } else {
                result.m_operation = RebalanceOperation::RESIZE_REBALANCE;
            }
        }
    }

    return result;
}

void PackedMemoryArray::rebalance_run_apma(RebalanceMetadata& action) {
    // hack: pretend the new element has already been inserted
    if(action.is_insert()){ m_storage.m_segment_sizes[action.m_insert_segment]++; }

    // detect the hammered segments/intervals
    size_t window_start = action.m_window_start;
    size_t window_length = action.m_operation == RebalanceOperation::REBALANCE ? action.m_window_length : m_storage.m_number_segments;
    Weights weights_builder{ *this, window_start, window_length };
    auto weights = weights_builder.release(); // hammered segments/intervals
    int wbalance = weights_builder.balance(); // = amount of hammer insertions minus amount of hammer deletions

    // hack: readjust the cardinalities
    if(action.is_insert()){ m_storage.m_segment_sizes[action.m_insert_segment]--; }

    MoveDetectorInfo mdi { *this, static_cast<size_t>( action.m_window_start ) }, *ptr_mdi = nullptr;
    if(action.m_operation == RebalanceOperation::REBALANCE){
        mdi.resize(2 * weights.size()); // it can move up to 2 * |weights| info
        ptr_mdi = &mdi;
    }

    set_thresholds(action); // update the thresholds of the calibrator tree

    AdaptiveRebalancing ar{ *this, move(weights), wbalance, (size_t) action.m_window_length, (size_t) action.get_cardinality_after(), ptr_mdi };
    action.m_apma_partitions = ar.release();
}


void PackedMemoryArray::do_rebalance(const RebalanceMetadata& action) {
    switch(action.m_operation){
    case RebalanceOperation::REBALANCE: {
        if (action.m_window_length < m_storage.get_segments_per_extent()){
            spread_local(action); // local to the extent
        } else { // use rewiring
            COUT_DEBUG("REBALANCE w/REWIRING, cardinality: " << action.get_cardinality_after() << ", window: [" << action.m_window_start << ", " << action.m_window_start + action.m_window_length << ")");
            SpreadWithRewiring instance{ this, (size_t) action.m_window_start, (size_t) action.m_window_length, action.m_apma_partitions };
            if(action.m_is_insert){ instance.set_element_to_insert(action.m_insert_key, action.m_insert_value); }
            instance.execute();
        }
    } break;
    case RebalanceOperation::RESIZE_REBALANCE: { // use rewiring
        COUT_DEBUG("REBALANCE RESIZE: cardinality: " << action.get_cardinality_after() << ", " << m_storage.m_number_segments << " -> " << action.m_window_length);
        m_detector.resize(action.m_window_length);
        resize_rebalance(action);

    } break;
    case RebalanceOperation::RESIZE: {
        m_detector.resize(action.m_window_length);
        resize(action);
    } break;
    default:
        assert(0 && "Invalid operation");
    }
}

/*****************************************************************************
 *                                                                           *
 *   Spread without rewiring                                                 *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::spread_local(const RebalanceMetadata& action){
    COUT_DEBUG("start: " << action.m_window_start << ", length: " << action.m_window_length);

    // workspace
    auto fn_deallocate = [this](void* ptr){ m_memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> input_keys_ptr{ m_memory_pool.allocate<int64_t>(action.get_cardinality_after()), fn_deallocate };
    int64_t* __restrict input_keys = input_keys_ptr.get();
    unique_ptr<int64_t, decltype(fn_deallocate)> input_values_ptr{ m_memory_pool.allocate<int64_t>(action.get_cardinality_after()), fn_deallocate };
    int64_t* __restrict input_values = input_values_ptr.get();

    // 1) first copy all elements in input keys
    int64_t insert_position = -1;
    spread_load(action, input_keys, input_values, &insert_position);

//    // debug only
//#if defined(DEBUG)
//    for(size_t i =0; i < action.get_cardinality_after(); i++){
//        cout << "Input [" << i << "] <" << input_keys[i] << ", " << input_values[i] << ">" << endl;
//    }
//#endif

    // 2) detector record
    spread_detector_record detector_record, *ptr_detector_record = nullptr;
    if(action.is_insert()){
        detector_record = spread_create_detector_record(input_keys, action.get_cardinality_after(), insert_position);
        ptr_detector_record = &detector_record;
    }

    // 3) copy the elements from input_keys to the final segments
    const auto& partitions = action.m_apma_partitions;
    size_t segment_id = 0;
    for(size_t i = 0, sz = partitions.size(); i < sz; i++){
        assert(partitions[i].m_segments > 0);
        size_t length = partitions[i].m_cardinality;
        if(partitions[i].m_segments == 1){ // copy a single segment
            spread_save(action.m_window_start + segment_id, input_keys, input_values, length, ptr_detector_record);
        } else {
            spread_save(action.m_window_start + segment_id, partitions[i].m_segments, input_keys, input_values, length, ptr_detector_record);
        }

        input_keys += length;
        input_values += length;
        segment_id += partitions[i].m_segments;

        // adjust the starting offset of the inserted key
        if (ptr_detector_record){
           detector_record.m_position -= length;
           if(detector_record.m_position < 0) ptr_detector_record = nullptr;
        }
    }
    assert(segment_id == action.m_window_length && "Not all segments visited");

    if(action.m_window_length == m_storage.m_number_segments)
        m_detector.clear();
}


size_t PackedMemoryArray::spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value){
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

    return i;
}

void PackedMemoryArray::spread_load(const RebalanceMetadata& action, int64_t* __restrict keys_to, int64_t* __restrict values_to, int64_t* output_position_key_inserted){
    // insert position
    int64_t position_key_inserted = 0;

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    int64_t* __restrict workspace_keys = m_storage.m_keys + action.m_window_start * m_storage.m_segment_capacity;
    int64_t* __restrict workspace_values = m_storage.m_values + action.m_window_start * m_storage.m_segment_capacity;
    segment_size_t* __restrict workspace_sizes = m_storage.m_segment_sizes + action.m_window_start;

    bool do_insert = action.is_insert();
    int64_t new_key_segment = do_insert ? (action.m_insert_segment - action.m_window_start) : -1;

    for(int64_t i = 1; i < action.m_window_length; i+=2){
        size_t length = workspace_sizes[i -1] + workspace_sizes[i];
        size_t offset = (m_storage.m_segment_capacity * i) - workspace_sizes[i-1];
        int64_t* __restrict keys_from = workspace_keys + offset;
        int64_t* __restrict values_from = workspace_values + offset;

        // destination
        if (new_key_segment == i || new_key_segment == i -1){
            position_key_inserted += spread_insert_unsafe(keys_from, values_from, keys_to, values_to, length, action.m_insert_key, action.m_insert_value);
            if(output_position_key_inserted) *output_position_key_inserted = position_key_inserted;
            do_insert = false;
            keys_to++; values_to++;
        } else {
            memcpy(keys_to, keys_from, sizeof(keys_to[0]) * length);
            memcpy(values_to, values_from, sizeof(values_from[0]) * length);
            if(do_insert) { position_key_inserted += length; } // the inserted key has not been yet inserted
        }

        keys_to += length; values_to += length;
    }

    if(do_insert){
        position_key_inserted += spread_insert_unsafe(nullptr, nullptr, keys_to, values_to, 0, action.m_insert_key, action.m_insert_value);
        if(output_position_key_inserted) *output_position_key_inserted = position_key_inserted;
    }
}

void PackedMemoryArray::spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record){
    assert(cardinality > 0 && "Empty segment");

    int64_t* keys_to = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* values_to = m_storage.m_values + segment_id * m_storage.m_segment_capacity;

    if(segment_id %2  == 0){ // even segment, adjust the base addresses
        keys_to += m_storage.m_segment_capacity - cardinality;
        values_to += m_storage.m_segment_capacity - cardinality;
    }

    memcpy(keys_to, keys_from, sizeof(keys_to[0]) * cardinality);
    memcpy(values_to, values_from, sizeof(values_to[0]) * cardinality);

    m_index.set_separator_key(segment_id, keys_from[0]);
    m_storage.m_segment_sizes[segment_id] = cardinality;

    if(detector_record && detector_record->m_position >= 0 && detector_record->m_position < cardinality)
        m_detector.insert(segment_id, detector_record->m_predecessor, detector_record->m_successor);
}

void PackedMemoryArray::spread_save(size_t window_start, size_t window_length, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record){
    int64_t* __restrict keys_to = m_storage.m_keys + window_start * m_storage.m_segment_capacity;
    int64_t* __restrict values_to = m_storage.m_values + window_start * m_storage.m_segment_capacity;
    uint16_t* __restrict segment_sizes = m_storage.m_segment_sizes + window_start;

    auto card_per_segment = cardinality / window_length;
    auto odd_segments = cardinality % window_length;

    // 1) handle the detector record
    if(detector_record && detector_record->m_position >= 0 && detector_record->m_position < cardinality){
        size_t detector_position = detector_record->m_position;
        size_t odd_segments_threshold = odd_segments * (card_per_segment +1);
        size_t segment_id;
        if(detector_position < odd_segments_threshold){
            segment_id = detector_position / (card_per_segment +1);
        } else {
            detector_position -= odd_segments_threshold;
            segment_id = odd_segments + detector_position / card_per_segment;
        }
        assert(segment_id < window_length && "Incorrect calculus");
        m_detector.insert(window_start + segment_id, detector_record->m_predecessor, detector_record->m_successor);
    }

    // 2) set the segment sizes
    assert((card_per_segment + (odd_segments >0)) <= m_storage.m_segment_capacity && "Segment overfilled");
    for(size_t i = 0; i < odd_segments; i++){
        segment_sizes[i] = card_per_segment +1;
    }
    for(size_t i = odd_segments; i < window_length; i++){
        segment_sizes[i] = card_per_segment;
    }

    // 3) copy the first segment if it's at an odd position
    if(window_start %2 == 1){
        size_t this_card = card_per_segment + odd_segments;
        memcpy(keys_to, keys_from, sizeof(keys_to[0]) * this_card);
        memcpy(values_to, values_from, sizeof(values_to[0]) * this_card);
        m_index.set_separator_key(window_start, keys_to[0]);

        window_start++;
        keys_to += m_storage.m_segment_capacity;
        values_to += m_storage.m_segment_capacity;
        keys_from += this_card;
        values_from += this_card;
        window_length--;
        if(odd_segments > 0) odd_segments--;
    }

    // 4) copy the bulk segments
    assert(window_length % 2 == 0 && "Expected an even position");
    for(size_t i = 1; i < window_length; i+=2){
        size_t card_left = card_per_segment + ((i -1) < odd_segments);
        size_t card_right = card_per_segment + ((i) < odd_segments);
        COUT_DEBUG("[bulk] i: " << i << ", card_left: " << card_left << ", card_right: " << card_right);

        size_t length = card_left + card_right;
        size_t offset = i * m_storage.m_segment_capacity - card_left;
        int64_t* keys_to_start = keys_to + offset;
        int64_t* values_to_start = values_to + offset;

        m_index.set_separator_key(window_start + i-1,  keys_from[0]);
        m_index.set_separator_key(window_start + i,   (keys_from + card_left)[0]);

        memcpy(keys_to_start, keys_from, length * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, length * sizeof(values_from[0]));

        keys_from += length;
        values_from += length;
    }

    // 5) copy the last segment, if it's at an even position
    if(window_length % 2 == 1){
        size_t offset = window_length * m_storage.m_segment_capacity - card_per_segment;
        int64_t* keys_to_start = keys_to + offset;
        int64_t* values_to_start = values_to + offset;

        m_index.set_separator_key(window_start + window_length -1,  keys_from[0]);

        memcpy(keys_to_start, keys_from, card_per_segment * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, card_per_segment * sizeof(values_from[0]));
    }
}

PackedMemoryArray::spread_detector_record
PackedMemoryArray::spread_create_detector_record(int64_t* keys, int64_t size, int64_t position){
    if(position < 0)
        return {-1, numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max() };

    int64_t predecessor = position > 0 ? keys[position -1] : numeric_limits<int64_t>::min();
    int64_t successor = position < (size -1) ? keys[position +1] : numeric_limits<int64_t>::max();

    return {position, predecessor, successor};
}

/*****************************************************************************
 *                                                                           *
 *   Resize using rewiring                                                   *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::resize_rebalance(const RebalanceMetadata& action) {
    const size_t num_segments_before = m_storage.m_number_segments;
    const size_t num_segments_after = action.m_window_length;
    COUT_DEBUG("segments: " << num_segments_before << " -> " << num_segments_after);

    // 1) Extend the PMA
    if(num_segments_after > num_segments_before){ m_storage.extend(num_segments_after - num_segments_before); }
    m_index.rebuild(num_segments_after);

    // 2) Spread
    SpreadWithRewiring rewiring_instance(this, 0, num_segments_after, action.m_apma_partitions );
    if(action.m_is_insert){ rewiring_instance.set_element_to_insert(action.m_insert_key, action.m_insert_value); }
    size_t start_position = (num_segments_before -1) * m_storage.m_segment_capacity + m_storage.m_segment_sizes[num_segments_before -1];
    rewiring_instance.set_absolute_position(start_position);
    rewiring_instance.execute();

    // 3) Shrink the PMA
    if(num_segments_after < num_segments_before){ m_storage.shrink(num_segments_before - num_segments_after); }
}

/*****************************************************************************
 *                                                                           *
 *   Full resize                                                             *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::resize(const RebalanceMetadata& action) {
    bool do_insert = action.m_is_insert;
    size_t num_segments = action.m_window_length; // new number of segments
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
    unique_ptr<PackedMemoryArray, decltype(xDeleter)> ixCleanup { this, xDeleter };
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
    struct {
        int index = 0; // current position in the vector partitions
        int segment = 0; // current segment considered
        int card_per_segment = 0; // cardinality per segment
        int odd_segments = 0; // number of segments with an additional element than `card_per_segment'
    } partition_state;
    const auto& partitions = action.m_apma_partitions;
    partition_state.card_per_segment = partitions[0].m_cardinality / partitions[0].m_segments;
    partition_state.odd_segments = partitions[0].m_cardinality % partitions[0].m_segments;

    for(size_t j = 0; j < num_segments; j++){
        // elements to copy
        size_t elements_to_copy = partition_state.card_per_segment + (partition_state.segment < partition_state.odd_segments);

//        COUT_DEBUG("j: " << j << ", elements_to_copy: " << elements_to_copy);

        size_t output_offset = output_segment_odd ? 0 : m_storage.m_segment_capacity - elements_to_copy;
        size_t output_canonical_index = j * m_storage.m_segment_capacity;
        int64_t* output_keys = xKeys + output_canonical_index + output_offset;
        int64_t* output_values = xValues + output_canonical_index + output_offset;
        xSizes[j] = elements_to_copy;
        if(input_size > 0) // protect from the edge case: the first segment will contain only one element, that is the new element to be inserted
            m_index.set_separator_key(j, input_keys[0]);

        do {
            assert(elements_to_copy <= m_storage.m_segment_capacity && "Overflow");
            assert(((input_size > 0) || (elements_to_copy == 1 && j == num_segments -1)) && "Empty input segment");
            size_t cpy1 = min(elements_to_copy, input_size);
            size_t input_copied, output_copied;
            if(do_insert && (input_size == 0 || action.m_insert_key <= input_keys[cpy1 -1])){
                // merge
                input_copied = max<int64_t>(0, static_cast<int64_t>(cpy1) -1); // min = 0
                output_copied = input_copied +1;
                size_t position = spread_insert_unsafe(input_keys, input_values, output_keys, output_values, input_copied, action.m_insert_key, action.m_insert_value);
                if(position == 0 && output_keys == xKeys + output_canonical_index + output_offset)
                    m_index.set_separator_key(j, action.m_insert_key);
                do_insert = false;
            } else {
                input_copied = output_copied = cpy1;
                memcpy(output_keys, input_keys, cpy1 * sizeof(m_storage.m_keys[0]));
                memcpy(output_values, input_values, cpy1 * sizeof(m_storage.m_values[0]));
            }
            assert(output_copied >= 1 && "Made no progress");
            output_keys += output_copied; input_keys += input_copied;
            output_values += output_copied; input_values += input_copied;
            input_size -= input_copied;

//            COUT_DEBUG("cpy1: " << cpy1 << ", elements_to_copy: " << elements_to_copy - cpy1 << ", input_size: " << input_size);

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

            elements_to_copy -= output_copied;
        } while(elements_to_copy > 0);

        // should we insert a new element in this bucket
        if(do_insert && action.m_insert_key < output_keys[-1]){
            int64_t predecessor, successor;
            auto min = m_storage.insert(j, action.m_insert_key, action.m_insert_value, &predecessor, &successor);
            m_detector.insert(j, predecessor, successor);
            if(min) m_index.set_separator_key(j, action.m_insert_key); // update the minimum in the B+ tree
            do_insert = false;
        }

        output_segment_odd = !output_segment_odd; // flip

        // move to the next segment
        partition_state.segment++;
        if(partition_state.segment >= partitions[partition_state.index].m_segments){
            partition_state.index++;
            partition_state.segment = 0;
            if(partition_state.index < partitions.size()){
                size_t cardinality = partitions[partition_state.index].m_cardinality;
                size_t num_segments = partitions[partition_state.index].m_segments;
                partition_state.card_per_segment = cardinality / num_segments;
                partition_state.odd_segments = cardinality % num_segments;
            }
        }
    }

    // if the element hasn't been inserted yet, it means it has to be placed in the last segment
    if(do_insert){
        int64_t predecessor, successor;
        auto min = m_storage.insert(num_segments -1, action.m_insert_key, action.m_insert_value, &predecessor, &successor);
        m_detector.insert(num_segments -1, predecessor, successor);
        if(min) m_index.set_separator_key(num_segments -1, action.m_insert_key); // update the minimum in the B+ tree
        do_insert = false;
    }

    // update the PMA properties
    m_storage.m_number_segments = num_segments;
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/
int PackedMemoryArray::find_position(size_t segment_id, int64_t key) const noexcept {
    if(key == std::numeric_limits<int64_t>::min()) return 0;
    size_t sz = m_storage.m_segment_sizes[segment_id];
    if(key == std::numeric_limits<int64_t>::max()) return static_cast<int>(sz);
    // in ::rebalance, we may temporary alter the size of a segment to segment_capacity +1
    sz = min<size_t>(sz, m_storage.m_segment_capacity); // avoid overflow

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int start, stop;

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), the keys are at the end
        start = m_storage.m_segment_capacity - sz;
        stop = m_storage.m_segment_capacity;
    } else { // odd segment ids (1, 3, ...), the keys are at the start of the segment
        start = 0;
        stop = sz;
    }

    for(int i = start; i < stop; i++){
        if(keys[i] == key){
            return i -start;
        }
    }

    return -1; // not found
}


int64_t PackedMemoryArray::find(int64_t key) const {
    // not worth merging the code with #find_position

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
unique_ptr<::pma::Iterator> PackedMemoryArray::empty_iterator() const{
    return make_unique<pma::adaptive::int3::Iterator>(m_storage);
}

unique_ptr<::pma::Iterator> PackedMemoryArray::find(int64_t min, int64_t max) const {
    if(empty()) return empty_iterator();
    return make_unique<pma::adaptive::int3::Iterator> (m_storage, m_index.find_first(min), m_index.find_last(max), min, max );
}
unique_ptr<::pma::Iterator> PackedMemoryArray::iterator() const {
    if(empty()) return empty_iterator();
    return make_unique<pma::adaptive::int3::Iterator> (m_storage, 0, m_storage.m_number_segments -1,
            numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max()
    );
}

/*****************************************************************************
 *                                                                           *
 *   Sum                                                                     *
 *                                                                           *
 *****************************************************************************/
::pma::Interface::SumResult PackedMemoryArray::sum(int64_t min, int64_t max) const {
    return do_sum(m_storage, m_index.find_first(min), m_index.find_last(max), min, max );
}

/*****************************************************************************
 *                                                                           *
 *   Segment statistics                                                      *
 *                                                                           *
 *****************************************************************************/
namespace {
    struct SegmentStatistics {
        uint64_t m_num_segments;
        uint64_t m_distance_avg;
        uint64_t m_distance_min;
        uint64_t m_distance_max;
        uint64_t m_distance_stddev;
        uint64_t m_distance_median;
        uint64_t m_cardinality_avg;
        uint64_t m_cardinality_min;
        uint64_t m_cardinality_max;
        uint64_t m_cardinality_stddev;
        uint64_t m_cardinality_median;
    };
}

decltype(auto) PackedMemoryArray::compute_segment_statistics() const {
    SegmentStatistics stats {m_storage.m_number_segments, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    // memory distances
    uint64_t distance_sum = 0;
    uint64_t distance_sum_sq = 0;
    uint64_t distance_min = -1;
    uint64_t distance_max = 0;
    uint64_t distance_gap_start = 0;
    vector<uint64_t> distances;
    distances.reserve(m_storage.m_number_segments / 2 /*-1*/);

    // cardinalities
    uint64_t cardinality_sum = m_storage.m_cardinality;
    uint64_t cardinality_sum_sq = 0;
    uint64_t cardinality_min = -1;
    uint64_t cardinality_max = 0;
    vector<uint64_t> cardinalities;
    cardinalities.reserve(m_storage.m_number_segments);


    for(size_t i = 0; i < m_storage.m_number_segments; i++){
        size_t segment_size = m_storage.m_segment_sizes[i];

        // memory distances
        if(i > 0){
            if (i % 2 == 0){
                uint64_t distance_gap_end = 2 * m_storage.m_segment_capacity - segment_size;
                uint64_t distance_current = (distance_gap_end - distance_gap_start) * sizeof(m_storage.m_keys[0]);
                distance_sum += distance_current;
                distance_sum_sq += distance_current * distance_current;
                if(distance_min > distance_current) distance_min = distance_current;
                if(distance_max < distance_current) distance_max = distance_current;
                distances.push_back(distance_current);
            } else { // odd segment
                distance_gap_start = segment_size;
            }
        }

        // cardinalities
        cardinality_sum_sq += (segment_size * segment_size);
        if(cardinality_min > segment_size) cardinality_min = segment_size;
        if(cardinality_max < segment_size) cardinality_max = segment_size;
        cardinalities.push_back(segment_size);
    }

    // distances
    size_t dist_sz = distances.size();
    if(dist_sz > 0){
        stats.m_distance_avg = distance_sum / dist_sz;
        stats.m_distance_max = distance_max;
        stats.m_distance_min = distance_min;
        stats.m_distance_stddev = sqrt( (static_cast<double>(distance_sum_sq) / dist_sz) -
                pow(stats.m_distance_avg, 2.0) );
        sort(begin(distances), end(distances));
        assert(dist_sz == m_storage.m_number_segments /2 -1);
        if(dist_sz % 2 == 1){
            stats.m_distance_median = distances[dist_sz /2];
        } else {
            size_t d1 = dist_sz /2;
            size_t d0 = d1 - 1;
            stats.m_distance_median = (distances[d0] + distances[d1]) / 2;
        }
    }

    // cardinalities
    stats.m_cardinality_avg = cardinality_sum / m_storage.m_number_segments;
    stats.m_cardinality_max = cardinality_max;
    stats.m_cardinality_min = cardinality_min;
    stats.m_cardinality_stddev = sqrt( (static_cast<double>(cardinality_sum_sq) / m_storage.m_number_segments) -
            pow(stats.m_cardinality_avg, 2.0) );

    // Compute the median
    sort(begin(cardinalities), end(cardinalities));
    size_t card_sz = cardinalities.size();
    assert(card_sz == m_storage.m_number_segments);
    if(card_sz % 2 == 1){
        stats.m_cardinality_median = cardinalities[card_sz /2];
    } else {
        size_t d1 = card_sz /2;
        size_t d0 = d1 - 1;
        stats.m_cardinality_median = (cardinalities[d0] + cardinalities[d1]) / 2;
    }

    return stats;
}

void PackedMemoryArray::record_segment_statistics() const {
    LOG_VERBOSE("[PackedMemoryArray] Computing segment statistics...");

    auto stats = compute_segment_statistics();

    LOG_VERBOSE("--> # segments: " << stats.m_num_segments);
    LOG_VERBOSE("--> distance average: " << stats.m_distance_avg << ", min: " << stats.m_distance_min << ", max: " << stats.m_distance_max << ", std. dev: " <<
            stats.m_distance_stddev << ", median: " << stats.m_distance_median);
    LOG_VERBOSE("--> cardinality average: " << stats.m_cardinality_avg << ", min: " << stats.m_cardinality_min << ", max: " << stats.m_cardinality_max << ", std. dev: " <<
            stats.m_cardinality_stddev << ", median: " << stats.m_cardinality_median);

    config().db()->add("btree_leaf_statistics")
                    ("num_leaves", stats.m_num_segments)
                    ("dist_avg", stats.m_distance_avg)
                    ("dist_min", stats.m_distance_min)
                    ("dist_max", stats.m_distance_max)
                    ("dist_stddev", stats.m_distance_stddev)
                    ("dist_median", stats.m_distance_median)
                    ("card_avg", stats.m_cardinality_avg)
                    ("card_min", stats.m_cardinality_min)
                    ("card_max", stats.m_cardinality_max)
                    ("card_stddev", stats.m_cardinality_stddev)
                    ("card_median", stats.m_cardinality_median)
                    ;
}

void PackedMemoryArray::set_record_segment_statistics(bool value) {
    m_segment_statistics = value;
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
std::ostream& operator<<(std::ostream& out, const PackedMemoryArray& pma){
    pma.dump(out);
    return out;
}

void PackedMemoryArray::dump() const {
    dump(cout);
}

void PackedMemoryArray::dump(std::ostream& out) const {
    bool integrity_check = true;

    m_index.dump(out, &integrity_check);

    out << "\n";

    dump_storage(out, &integrity_check);

    out << "\n";

    m_detector.dump(out);

    assert(integrity_check && "Integrity check failed!");
}

void PackedMemoryArray::dump_storage(std::ostream& out, bool* integrity_check) const {
    out << "[PMA] cardinality: " << m_storage.m_cardinality << ", capacity: " << m_storage.capacity() << ", " <<
            "height: "<< m_storage.hyperheight() << ", #segments: " << m_storage.m_number_segments <<
            ", blksz #elements: " << m_storage.m_segment_capacity << ", pages per extent: " << m_storage.m_pages_per_extent <<
            ", # segments for balanced thresholds: " << balanced_thresholds_cutoff() << endl;

    if(empty()){ // edge case
        out << "-- empty --" << endl;
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

}}} // pma::adaptive::int3
