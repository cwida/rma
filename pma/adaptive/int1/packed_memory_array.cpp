/*
 * packed_memory_array.cpp
 *
 *  Created on: Mar 7, 2018
 *      Author: dleo@cwi.nl
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
#include "configuration.hpp"
#include "database.hpp"
#include "iterator.hpp"
#include "miscellaneous.hpp"
#include "move_detector_info.hpp"
#include "sum.hpp"
#include "weights.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

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

#if defined(PROFILING)
#define PROFILER_INIT auto _profiler = m_rebalancing_profiler.profiler(is_insert);
#define PROFILER_START(action) _profiler.action##_start()
#define PROFILER_STOP(action) _profiler.action##_stop()
#define PROFILER_SEARCH_START PROFILER_START(search);
#define PROFILER_SEARCH_STOP PROFILER_STOP(search);
#define PROFILER_APMA_START PROFILER_START(apma);
#define PROFILER_APMA_STOP PROFILER_STOP(apma);
#define PROFILER_SPREAD_START _profiler.spread_start(window_length);
#define PROFILER_SPREAD_STOP PROFILER_STOP(spread);
#define PROFILER_RESIZE_START _profiler.resize_start(m_storage.m_capacity, is_insert ? (m_storage.m_capacity * 2) : (m_storage.m_capacity /2));
#define PROFILER_RESIZE_STOP PROFILER_STOP(resize);
#else
#define PROFILER_INIT
#define PROFILER_SEARCH_START
#define PROFILER_SEARCH_STOP
#define PROFILER_APMA_START
#define PROFILER_APMA_STOP
#define PROFILER_SPREAD_START
#define PROFILER_SPREAD_STOP
#define PROFILER_RESIZE_START
#define PROFILER_RESIZE_STOP
#endif


/*****************************************************************************
 *                                                                           *
 *   Initialization                                                          *
 *                                                                           *
 *****************************************************************************/

PackedMemoryArray::PackedMemoryArray() : PackedMemoryArray(/* B = */ 64) { }
PackedMemoryArray::PackedMemoryArray(size_t btree_block_size) : PackedMemoryArray(btree_block_size, btree_block_size) { }
PackedMemoryArray::PackedMemoryArray(size_t btree_block_size, size_t pma_segment_size) :
       m_index(btree_block_size),
       m_storage(pma_segment_size),
       m_detector(m_knobs, 1, 8),
       m_random_sampler(chrono::system_clock::now().time_since_epoch().count()) {
}

PackedMemoryArray::~PackedMemoryArray() {
    if(m_segment_statistics) record_segment_statistics();

    free(m_storage.m_keys); m_storage.m_keys = nullptr;
    free(m_storage.m_values); m_storage.m_values = nullptr;
    free(m_storage.m_segment_sizes); m_storage.m_segment_sizes = nullptr;
    free(m_index.m_keys); m_index.m_keys = nullptr;

#if defined(PROFILING)
    m_rebalancing_profiler.save_results();
#endif
}

/*****************************************************************************
 *                                                                           *
 *   Rebalancing                                                             *
 *                                                                           *
 *****************************************************************************/
bool PackedMemoryArray::rebalance(size_t segment_id, int64_t* key, int64_t* value){
    assert(((key && value) || (!key && !value)) && "Either both key & value are specified (insert) or none of them is (delete)");
    const bool is_insert = key != nullptr;
    PROFILER_INIT

    PROFILER_SEARCH_START
    COUT_DEBUG("segment_id: " << segment_id);
    size_t num_elements_after = is_insert ? m_storage.m_segment_capacity +1 : m_storage.m_segment_sizes[segment_id];
    // these inits are only valid for the edge case that the calibrator tree has height 1, i.e. the data structure contains only one segment
    double rho = 0.0, theta = 1.0, density = static_cast<double>(num_elements_after)/m_storage.m_segment_capacity;
    size_t height = 1;
    COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta << ", num_elements: " << num_elements_after);

    int window_length = 1;
    int window_id = segment_id;
    int window_start = segment_id, window_end = segment_id;

    if(m_storage.m_height > 1){
        // find the bounds of this window
        int index_left = segment_id -1;
        int index_right = segment_id +1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;
            auto density_bounds = thresholds(height);
            rho = density_bounds.first;
            theta = density_bounds.second;

            // find the number of elements in the interval
            while(index_left >= window_start){
                num_elements_after += m_storage.m_segment_sizes[index_left];
                index_left--;
            }
            while(index_right < window_end){
                num_elements_after += m_storage.m_segment_sizes[index_right];
                index_right++;
            }

            COUT_DEBUG("num_elements: " << num_elements_after << ", window_start: " << window_start << ", window_length: " << window_length << ", segment_capacity: " << m_storage.m_segment_capacity);
            density = ((double) num_elements_after) / (window_length * m_storage.m_segment_capacity);

            COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta);
        } while ( ((is_insert && density > theta) || (!is_insert && density < rho)) && height < m_storage.m_height );
    }
    PROFILER_SEARCH_STOP

    // do we need a spread or a resize operation?
    const bool is_spread = (is_insert && density <= theta) || (!is_insert && density >= rho);
    const bool is_resize = !is_spread; // of course, either one or the other!

    // real number of elements, without the elements to be inserted
    const size_t num_elements_current = is_insert ? num_elements_after -1 : num_elements_after;
    spread_insertion insert_payload, *ptr_insert_payload = nullptr;
    if(is_spread && is_insert){
        insert_payload.m_key = *key; insert_payload.m_value = *value; insert_payload.m_segment = segment_id;
        ptr_insert_payload = &insert_payload;
        m_storage.m_segment_sizes[segment_id]++; // pretend the elements has already been inserted
    }

    // adaptive strategy
    PROFILER_APMA_START
    Weights weights_builder{ *this, (size_t) window_start, (size_t) window_length };
    auto weights = weights_builder.release();

    // hack: readjust the cardinalities
    if(is_spread && is_insert){ m_storage.m_segment_sizes[segment_id]--; }

    if(weights.empty() && is_spread){ // shortcut, traditional strategy for spread
        PROFILER_APMA_STOP
        COUT_DEBUG("-- SPREAD EVENLY --");
        PROFILER_SPREAD_START
        spread_evenly(window_start, window_length, num_elements_current, ptr_insert_payload);
        PROFILER_SPREAD_STOP
    } else {
        auto wbalance = weights_builder.balance();

        size_t ar_cardinality = num_elements_after;
        size_t ar_window_length = window_length;
        if(is_resize) {
            if(is_insert) {
                ar_window_length *= 2;
                thresholds(1, m_storage.m_height +1); // regenerate the thresholds
            } else {
                ar_window_length /= 2;
                thresholds(1, m_storage.m_height -1); // regenerate the thresholds
            }
            ar_cardinality = num_elements_current;
        }

        MoveDetectorInfo mdi { *this, static_cast<size_t>( window_start ) }, *ptr_mdi = nullptr;
        if(is_spread && window_length != m_storage.m_number_segments){
            mdi.resize(weights.size());
            ptr_mdi = &mdi;
        }

        AdaptiveRebalancing ar{ *this, move(weights), wbalance, ar_window_length, ar_cardinality, ptr_mdi };
        auto partitions = ar.release();
        PROFILER_APMA_STOP

        if(is_spread) {
            COUT_DEBUG("-- SPREAD UNEVENLY --");
            PROFILER_SPREAD_START
            spread_unevenly(partitions, window_start, window_length, num_elements_current, ptr_insert_payload);
            PROFILER_SPREAD_STOP
        } else {
            COUT_DEBUG("-- RESIZE --");
            PROFILER_RESIZE_START
            resize(partitions, is_insert);
            PROFILER_RESIZE_STOP

            // try again now
//            if(is_insert) { insert(*key, *value); }
            // rather than inserting here, allow to release the memory pool
        }
    }

    return is_spread && is_insert;
}


void PackedMemoryArray::resize(const VectorOfPartitions& partitions, const bool is_insert) {
    size_t capacity = is_insert ?  m_storage.m_capacity * 2 : m_storage.m_capacity / 2; // new capacity
    size_t num_segments = capacity / m_storage.m_segment_capacity;
    COUT_DEBUG(m_storage.m_capacity << " --> " << capacity << ", num_segments: " << num_segments);

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_sizes) ixSizes;
    m_storage.alloc_workspace(num_segments, &ixKeys, &ixValues, &ixSizes);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_sizes);
    auto xFreePtr = [](void* ptr){ free(ptr); };
    unique_ptr<int64_t, decltype(xFreePtr)> ixKeys_ptr { ixKeys, xFreePtr };
    unique_ptr<int64_t, decltype(xFreePtr)> ixValues_ptr{ ixValues, xFreePtr };
    unique_ptr<remove_pointer_t<decltype(m_storage.m_segment_sizes)>, decltype(xFreePtr)> ixSizes_ptr{ ixSizes, xFreePtr };
    int64_t* __restrict xKeys = m_storage.m_keys;
    int64_t* __restrict xValues = m_storage.m_values;
    decltype(m_storage.m_segment_sizes) __restrict xSizes = m_storage.m_segment_sizes;

    { // rebuild the index
        int k = ceil( log2(num_segments) / log2( m_index.B ) ); // log_B(N)
        double x = 1.0 / m_index.B;
        double x_k = pow(x, k);
        m_index.m_capacity = ceil( ((double) num_segments) * (x * (1 - x_k)) / (1 - x) ); // geometric series
        COUT_DEBUG("B+ Tree, m_index.m_capacity: " << m_index.m_capacity);
    }
    m_index.realloc_keys();
    if(num_segments <= m_index.B){
        m_index.m_offset_leaves = 0;
    } else {
        m_index.m_offset_leaves = m_index.m_capacity * m_index.B - num_segments;
    }
    m_index.m_height = ceil(log2(num_segments) / log2(m_index.B));
    COUT_DEBUG("B+ Tree, base: " << m_index.B << ", num_segments: " << num_segments);
    m_index.m_cardinality_root = num_segments > 1 ? num_segments / pow(m_index.B, m_index.m_height -1) -1 : 0;
    COUT_DEBUG("B+ Tree, m_index.m_cardinality_root: " << m_index.m_cardinality_root);

    // fetch the first non-empty input segment
    size_t input_segment_id = 0;
    size_t input_size = ixSizes[0];
    int64_t* input_keys = ixKeys + m_storage.m_segment_capacity;
    int64_t* input_values = ixValues + m_storage.m_segment_capacity;
    bool input_segment_odd = false; // consider '0' as even
    if(input_size == 0){ // corner case, the first segment is empty!
        assert(!is_insert && "Otherwise we shouldn't see empty segments");
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
    partition_state.card_per_segment = partitions[0].m_cardinality / partitions[0].m_segments;
    partition_state.odd_segments = partitions[0].m_cardinality % partitions[0].m_segments;

    for(size_t j = 0; j < num_segments; j++){
        // elements to copy
        size_t elements_to_copy = partition_state.card_per_segment + (partition_state.segment < partition_state.odd_segments);

        COUT_DEBUG("j: " << j << ", elements_to_copy: " << elements_to_copy);

        size_t output_offset = output_segment_odd ? 0 : m_storage.m_segment_capacity - elements_to_copy;
        size_t output_canonical_index = j * m_storage.m_segment_capacity;
        int64_t* output_keys = xKeys + output_canonical_index + output_offset;
        int64_t* output_values = xValues + output_canonical_index + output_offset;
        xSizes[j] = elements_to_copy;
        set_pivot(j, input_keys[0]);

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
                        assert(!is_insert && "Otherwise we shouldn't see empty segments");
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

    // update the PMA properties
    m_storage.m_capacity = capacity;
    m_storage.m_number_segments = num_segments;
    m_storage.m_height = log2(num_segments) +1;

    // rebuild the detector
    m_detector.resize(num_segments);

    // side effect: regenerate the thresholds
    thresholds(m_storage.m_height, m_storage.m_height);
}

size_t PackedMemoryArray::spread_insert_unsafe(spread_insertion* insert, int64_t* keys_to, int64_t* keys_from, int64_t* values_to, int64_t* values_from, int length){
    assert(insert != nullptr);
    COUT_DEBUG("segment: " << insert->m_segment << ", key: " << insert->m_key << ", length: " << length);

    int64_t new_key = insert->m_key;
//    size_t segment_id = insert->m_segment;

    int j = 0;
    while(j < length && new_key > keys_from[j]){
        keys_to[j] = keys_from[j];
        values_to[j] = values_from[j];
        j++;
    }

    keys_to[j] = new_key;
    values_to[j] = insert->m_value;

    keys_to++; values_to++;
    memcpy(keys_to +j, keys_from +j, max<int>(0, length -j) * sizeof(keys_to[0]));
    memcpy(values_to +j, values_from +j, max<int>(0, length -j) * sizeof(values_to[0]));

    m_storage.m_cardinality += 1;

    return j;
}

void PackedMemoryArray::spread_load(size_t segment_start, size_t segment_length, int64_t* __restrict keys_to, int64_t* __restrict values_to, spread_insertion* insert){
    // insert position
    int64_t position_key_inserted = 0;

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    int64_t* __restrict workspace_keys = m_storage.m_keys + segment_start * m_storage.m_segment_capacity;
    int64_t* __restrict workspace_values = m_storage.m_values + segment_start * m_storage.m_segment_capacity;
    segment_size_t* __restrict workspace_sizes = m_storage.m_segment_sizes + segment_start;

    int64_t new_key_segment = insert ? (insert->m_segment - segment_start) : -1;

    for(int64_t i = 1; i < segment_length; i+=2){
        size_t length = workspace_sizes[i -1] + workspace_sizes[i];
        size_t offset = (m_storage.m_segment_capacity * i) - workspace_sizes[i-1];
        int64_t* __restrict keys_from = workspace_keys + offset;
        int64_t* __restrict values_from = workspace_values + offset;

        // destination
        if (new_key_segment == i || new_key_segment == i -1){
            position_key_inserted += spread_insert_unsafe(insert, keys_to, keys_from, values_to, values_from, length);
            insert->m_segment = position_key_inserted; // a bit of a hack, use m_segment to state the absolute position rather than the actual segment
            insert = nullptr;
            keys_to++; values_to++;
        } else {
            memcpy(keys_to, keys_from, sizeof(keys_to[0]) * length);
            memcpy(values_to, values_from, sizeof(values_from[0]) * length);
            if(insert) { position_key_inserted += length; } // the inserted key has not been yet inserted
        }

        keys_to += length; values_to += length;
    }

    if(insert != nullptr){
        position_key_inserted += spread_insert_unsafe(insert, keys_to, nullptr, values_to, nullptr, 0);
        insert->m_segment = position_key_inserted;
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

    set_pivot(segment_id, keys_from[0]);
    m_storage.m_segment_sizes[segment_id] = cardinality;

    if(detector_record && detector_record->m_position >= 0 && detector_record->m_position < cardinality && record_sample_update())
        m_detector.insert(segment_id, detector_record->m_predecessor, detector_record->m_successor);
}


void PackedMemoryArray::spread_save(size_t segment_start, size_t segment_length, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record){
    int64_t* __restrict keys_to = m_storage.m_keys + segment_start * m_storage.m_segment_capacity;
    int64_t* __restrict values_to = m_storage.m_values + segment_start * m_storage.m_segment_capacity;
    uint16_t* __restrict segment_sizes = m_storage.m_segment_sizes + segment_start;

    auto card_per_segment = cardinality / segment_length;
    auto odd_segments = cardinality % segment_length;

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
        assert(segment_id < segment_length && "Incorrect calculus");
        if(record_sample_update())
            m_detector.insert(segment_start + segment_id, detector_record->m_predecessor, detector_record->m_successor);
    }

    // 2) set the segment sizes
    for(size_t i = 0; i < odd_segments; i++){
        segment_sizes[i] = card_per_segment +1;
    }
    for(size_t i = odd_segments; i < segment_length; i++){
        segment_sizes[i] = card_per_segment;
    }

    // 3) copy the first segment if it's at an odd position
    if(segment_start %2 == 1){
        size_t this_card = card_per_segment + odd_segments;
        memcpy(keys_to, keys_from, sizeof(keys_to[0]) * this_card);
        memcpy(values_to, values_from, sizeof(values_to[0]) * this_card);
        set_pivot(segment_start, keys_to[0]);

        segment_start++;
        keys_to += m_storage.m_segment_capacity;
        values_to += m_storage.m_segment_capacity;
        keys_from += this_card;
        values_from += this_card;
        segment_length--;
        if(odd_segments > 0) odd_segments--;
    }

    // 3) copy the bulk segments
    assert(segment_start % 2 == 0 && "Expected an even position");
    for(size_t i = 1; i < segment_length; i+=2){
        size_t card_left = card_per_segment + ((i -1) < odd_segments);
        size_t card_right = card_per_segment + ((i) < odd_segments);
        COUT_DEBUG("[bulk] i: " << i << ", card_left: " << card_left << ", card_right: " << card_right);

        size_t length = card_left + card_right;
        size_t offset = i * m_storage.m_segment_capacity - card_left;
        int64_t* keys_to_start = keys_to + offset;
        int64_t* values_to_start = values_to + offset;

        set_pivot(segment_start + i-1,  keys_from[0]);
        set_pivot(segment_start + i,   (keys_from + card_left)[0]);

        memcpy(keys_to_start, keys_from, length * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, length * sizeof(values_from[0]));

        keys_from += length;
        values_from += length;
    }

    // 4) copy the last segment, if it's at an even position
    if(segment_length % 2 == 1){
        size_t offset = segment_length * m_storage.m_segment_capacity - card_per_segment;
        int64_t* keys_to_start = keys_to + offset;
        int64_t* values_to_start = values_to + offset;

        set_pivot(segment_start + segment_length -1,  keys_from[0]);

        memcpy(keys_to_start, keys_from, card_per_segment * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, card_per_segment * sizeof(values_from[0]));
    }
}

void PackedMemoryArray::spread_evenly(size_t segment_start, size_t num_segments, size_t cardinality, spread_insertion* insert){
    COUT_DEBUG("start: " << segment_start << ", length: " << num_segments);

    // adjust the cardinality to include the element to be inserted
    if(insert) cardinality++;

    // workspace
    auto fn_deallocate = [this](void* ptr){ m_memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> input_keys_ptr{ m_memory_pool.allocate<int64_t>(cardinality), fn_deallocate };
    int64_t* __restrict input_keys = input_keys_ptr.get();
    unique_ptr<int64_t, decltype(fn_deallocate)> input_values_ptr{ m_memory_pool.allocate<int64_t>(cardinality), fn_deallocate };
    int64_t* __restrict input_values = input_values_ptr.get();

    // 1) first copy all elements in input keys
    spread_load(segment_start, num_segments, input_keys, input_values, insert);

    // 2) handle the detector
    spread_detector_record detector_record, *ptr_detector_record = nullptr;
    if(insert){
        detector_record = spread_create_detector_record(input_keys, cardinality, insert->m_segment);
        ptr_detector_record = &detector_record;
    }

    // 3) copy the elements from input_keys to the final segments
    spread_save(segment_start, num_segments, input_keys, input_values, cardinality, ptr_detector_record);

    // 4) reset the predictor/detector
    if(num_segments == m_storage.m_number_segments)
        m_detector.clear();
}

PackedMemoryArray::spread_detector_record
PackedMemoryArray::spread_create_detector_record(int64_t* keys, int64_t size, int64_t position){
    if(position < 0)
        return {-1, numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max() };

    int64_t predecessor = position > 0 ? keys[position -1] : numeric_limits<int64_t>::min();
    int64_t successor = position < (size -1) ? keys[position +1] : numeric_limits<int64_t>::max();

    return {position, predecessor, successor};
}


void PackedMemoryArray::spread_unevenly(const VectorOfPartitions& partitions, size_t segment_start, size_t num_segments, size_t cardinality /* excl. new key */, spread_insertion* insert){
    COUT_DEBUG("start: " << segment_start << ", length: " << num_segments);

    // adjust the cardinality to include the element to be inserted
    if(insert) cardinality++;

    // workspace
    auto fn_deallocate = [this](void* ptr){ m_memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> input_keys_ptr{ m_memory_pool.allocate<int64_t>(cardinality), fn_deallocate };
    int64_t* __restrict input_keys = input_keys_ptr.get();
    unique_ptr<int64_t, decltype(fn_deallocate)> input_values_ptr{ m_memory_pool.allocate<int64_t>(cardinality), fn_deallocate };
    int64_t* __restrict input_values = input_values_ptr.get();

    // 1) first copy all elements in input keys
    spread_load(segment_start, num_segments, input_keys, input_values, insert);

//    // debug only
//#if defined(DEBUG)
//    for(size_t i =0; i < cardinality; i++){
//        cout << "Input [" << i << "] <" << input_keys[i] << ", " << input_values[i] << ">" << endl;
//    }
//#endif

    // 2) detector record
    spread_detector_record detector_record, *ptr_detector_record = nullptr;
    if(insert){
        detector_record = spread_create_detector_record(input_keys, cardinality, insert->m_segment /* that's the abs. position actually */);
        ptr_detector_record = &detector_record;
    }

    // 3) copy the elements from input_keys to the final segments
    size_t segment_id = 0;
    for(size_t i = 0, sz = partitions.size(); i < sz; i++){
        assert(partitions[i].m_segments > 0);
        size_t length = partitions[i].m_cardinality;
        if(partitions[i].m_segments == 1){ // copy a single segment
            spread_save(segment_start + segment_id, input_keys, input_values, length, ptr_detector_record);
        } else {
            spread_save(segment_start + segment_id, partitions[i].m_segments, input_keys, input_values, length, ptr_detector_record);
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
    assert(segment_id == num_segments && "Not all segments visited");

    if(num_segments == m_storage.m_number_segments)
        m_detector.clear();
}


/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/
size_t PackedMemoryArray::size() const noexcept {
    return m_storage.m_cardinality;
}

bool PackedMemoryArray::empty() const noexcept {
    return size() == 0;
}

std::pair<double, double> PackedMemoryArray::thresholds(int height) {
    return thresholds(height, m_storage.m_height);
}

std::pair<double, double> PackedMemoryArray::thresholds(int node_height, int tree_height) {
    return m_density_bounds.thresholds(tree_height, node_height);
}

CachedMemoryPool& PackedMemoryArray::memory_pool() {
    return m_memory_pool;
}

Detector& PackedMemoryArray::detector(){
    return m_detector;
}

CachedDensityBounds& PackedMemoryArray::densities(){
    return m_density_bounds;
}

Knobs& PackedMemoryArray::knobs(){
    return m_knobs;
}

size_t PackedMemoryArray::get_segment_capacity() const noexcept {
    return m_storage.m_segment_capacity;
}

uniform_int_distribution<int> PackedMemoryArray::m_sampling_distribution{1, 100};

bool PackedMemoryArray::record_sample_update() noexcept {
    auto smpl_perc = m_knobs.get_sampling_percentage();
    return smpl_perc == 100 /* 100% */ ||  m_sampling_distribution(m_random_sampler) <= smpl_perc;
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::insert(int64_t key, int64_t value){
    if(UNLIKELY( empty() )){
        insert_empty(key, value);
    } else {
        size_t segment = find_bucket_any(key);
        insert_common(segment, key, value);
    }

#if defined(DEBUG)
    dump();
#endif
}


void PackedMemoryArray::insert_empty(int64_t key, int64_t value){
    assert(empty());
    assert(m_storage.m_capacity > 0 && "The storage does not have any capacity?");

    m_index.m_key_minimum = key;
    m_index.m_cardinality_root = 0; // there is still only a segment
    m_storage.m_segment_sizes[0] = 1;
    size_t pos = m_storage.m_segment_capacity -1;
    m_storage.m_keys[pos] = key;
    m_storage.m_values[pos] = value;
    m_storage.m_cardinality = 1;
}

void PackedMemoryArray::insert_common(size_t segment_id, int64_t key, int64_t value){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_capacity && "Overflow: attempting to access an invalid segment in the PMA");

    COUT_DEBUG("segment_id: " << segment_id << ", element: <" << key << ", " << value << ">");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        // attempt #1: it may fail in case resizing
        bool element_inserted = rebalance(segment_id, &key, &value);
        assert(m_memory_pool.empty() && "Memory leak!");
        // attempt #2: try again!
        if(!element_inserted) insert(key, value);
    } else { // find a spot where to insert this element
        bool minimum_updated = storage_insert_unsafe(segment_id, key, value);

        // have we just updated the minimum ?
        if (minimum_updated) set_pivot(segment_id, key);
    }
}

bool PackedMemoryArray::storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value){
    if(record_sample_update())
        return storage_insert_unsafe0<true>(segment_id, key, value);
    else
        return storage_insert_unsafe0<false>(segment_id, key, value);
}

template<bool record_update>
bool PackedMemoryArray::storage_insert_unsafe0(size_t segment_id, int64_t key, int64_t value){
    assert(m_storage.m_segment_sizes[segment_id] < m_storage.m_segment_capacity && "This segment is full!");

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    bool minimum = false; // the inserted key is the new minimum ?
    size_t sz = m_storage.m_segment_sizes[segment_id];
    int64_t predecessor, successor; // to update the detector

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), insert at the end of the segment
        size_t stop = m_storage.m_segment_capacity -1;
        size_t start = m_storage.m_segment_capacity - sz -1;
        size_t i = start;

        while(i < stop && keys[i+1] < key){
            keys[i] = keys[i+1];
            i++;
        }

        COUT_DEBUG("(even) segment_id: " << segment_id << ", start: " << start << ", stop: " << stop << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = start; j < i; j++){
            values[j] = values[j+1];
        }
        values[i] = value;

        minimum = (i == start);
        bool maximum = (i == stop);

        // update the detector
        predecessor = minimum ? std::numeric_limits<int64_t>::min() : keys[i -1];
        successor = maximum ? std::numeric_limits<int64_t>::max() : keys[i +1];

    } else { // for odd segment ids (1, 3, ...), insert at the front of the segment
        size_t i = sz;
        while(i > 0 && keys[i-1] > key){
            keys[i] = keys[i-1];
            i--;
        }

        COUT_DEBUG("(odd) segment_id: " << segment_id << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = sz; j > i; j--){
            values[j] = values[j-1];
        }
        values[i] = value;

        minimum = (i == 0);
        bool maximum = (i == sz);

        // update the detector
        predecessor = minimum ? std::numeric_limits<int64_t>::min() : keys[i -1];
        successor = maximum ? std::numeric_limits<int64_t>::max() : keys[i +1];
    }

    if(record_update) // detector
        m_detector.insert(segment_id, predecessor, successor);

    // update the cardinality
    m_storage.m_segment_sizes[segment_id]++;
    m_storage.m_cardinality += 1;

    return minimum;
}

void PackedMemoryArray::set_pivot(size_t segment_id, int64_t key){
    if(segment_id == 0) {
        m_index.m_key_minimum = key;
        return;
    }

    size_t base = m_index.m_offset_leaves;
    size_t capacity_level = m_index.m_capacity * m_index.B - base;
    size_t offset = segment_id;

    // move one level up?
    int depth = m_index.m_height -1;
    while (offset % m_index.B == 0 && depth > 0){ // at depth == 1 there is the root
        if(depth == 1){
            base = 0;
        } else {
            capacity_level /= m_index.B;
            base -= capacity_level;
        }
        offset /= m_index.B;
        COUT_DEBUG("depth: " << depth << ", base: " << base << ", position: " << offset);

        depth--;
    }

    COUT_DEBUG("segment_id: " << segment_id << ", key: " << key << ", absolute position: " << base + offset -1 << ", offset: " << offset -1);
    m_index.m_keys[base + offset -1] = key;
}

int64_t PackedMemoryArray::get_pivot(size_t segment_id) const {
    if(segment_id == 0) return m_index.m_key_minimum;
    assert(segment_id < m_storage.m_number_segments);

    size_t base = m_index.m_offset_leaves;
    size_t capacity_level = m_index.m_capacity * m_index.B - base;
    size_t offset = segment_id;

    // move one level up?
    int depth = m_index.m_height -1;
    while (offset % m_index.B == 0 && depth > 0){ // at depth == 1 there is the root
        if(depth == 1){
            base = 0;
        } else {
            capacity_level /= m_index.B;
            base -= capacity_level;
        }
        offset /= m_index.B;

        depth--;
    }

    return m_index.m_keys[base + offset -1];
}

/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t PackedMemoryArray::remove(int64_t key){
    if(empty()) return -1;

    auto segment_id = find_bucket_any(key);
    COUT_DEBUG("key: " << key << ", bucket: " << segment_id);
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
                    m_index.m_key_minimum = numeric_limits<decltype(m_index.m_key_minimum)>::min();
                    m_index.m_cardinality_root = 0;
                } else if(sz > 0) { // otherwise we are going to rebalance this segment anyway
                    set_pivot(segment_id, keys[imin +1]);
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
                set_pivot(segment_id, keys[0]);
            }
        } // end if (found)
    } // end if (odd segment)

    if(value != -1){ // found a value ?
        // detector
        if(record_sample_update())
            m_detector.remove(segment_id, predecessor, successor);

        // shall we rebalance ?
        if(m_storage.m_number_segments > 1){
            const size_t minimum_size = max<size_t>(thresholds(1).first * m_storage.m_segment_capacity, 1); // at least one element per segment
            if(sz < minimum_size){ rebalance(segment_id, nullptr, nullptr); }
        }
    }

#if defined(DEBUG)
    dump();
#endif

    return value;
}

/*****************************************************************************
 *                                                                           *
 *   Search                                                                  *
 *                                                                           *
 *****************************************************************************/
int PackedMemoryArray::find_key(size_t segment_id, int64_t key) const noexcept {
    if(key == std::numeric_limits<int64_t>::min()) return 0;
    size_t sz = min(m_storage.m_segment_sizes[segment_id], m_storage.m_segment_capacity); // avoid overflowing, ::rebalance temporarily alters the segment_size +1
    if(key == std::numeric_limits<int64_t>::max()) return static_cast<int>(sz);
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

size_t PackedMemoryArray::find_bucket_any(int64_t key) const noexcept {
    int64_t* __restrict keys = m_index.m_keys;

    // start from the root
    size_t offset = scan_node_any(keys, m_index.m_cardinality_root, key);

    // percolate in the tree
    int height = 2;
    size_t base = m_index.B;
    size_t level_sz = m_index.m_cardinality_root +1;
    while(height <= m_index.m_height){
        COUT_DEBUG("height: "<< height << ", m_index.m_height = " << m_index.m_height << ", base: " << base);
        offset *= m_index.B;
        int64_t* node = keys + base + offset;

        auto pos = scan_node_any(node, m_index.B -1, key);
        offset += pos;

        // update the base
        level_sz *= m_index.B;
        base += level_sz;
        height++;
    }

    COUT_DEBUG("bucket: " << offset);
    return offset;
}

size_t PackedMemoryArray::find_bucket_min(int64_t key) const noexcept {
    int64_t* __restrict keys = m_index.m_keys;

    // start from the root
    size_t offset = scan_node_min(keys, m_index.m_cardinality_root, key);

    // percolate in the tree
    int height = 2;
    size_t base = m_index.B;
    size_t level_sz = m_index.m_cardinality_root +1;
    while(height <= m_index.m_height){
        offset *= m_index.B;
        int64_t* node = keys + base + offset;
        COUT_DEBUG("height: "<< height << ", m_index.m_height = " << m_index.m_height << ", base: " << base << ", offset: " << offset << ", first key: " << node[0]);

        auto pos = scan_node_min(node, m_index.B -1, key);
        offset += pos;

        // update the base
        level_sz *= m_index.B;
        base += level_sz;
        height++;
    }

    COUT_DEBUG("bucket: " << offset);
    return offset;
}

size_t PackedMemoryArray::find_bucket_max(int64_t key) const noexcept {
    int64_t* __restrict keys = m_index.m_keys;

    // start from the root
    size_t offset = scan_node_max(keys, m_index.m_cardinality_root, key);
    COUT_DEBUG("key: " << key << ", height: 1, i: " << offset);

    // percolate in the tree
    int height = 2;
    size_t base = m_index.B;
    size_t level_sz = m_index.m_cardinality_root +1;
    while(height <= m_index.m_height){
        COUT_DEBUG("height: "<< height << ", m_index.m_height = " << m_index.m_height << ", base: " << base);
        offset *= m_index.B;
        int64_t* node = keys + base + offset;

        auto pos = scan_node_max(node, m_index.B -1, key);
        offset += pos;
        COUT_DEBUG("height: " << height << ", i: "  << pos << ", offset: " << offset);

        // update the base
        level_sz *= m_index.B;
        base += level_sz;
        height++;
    }

    COUT_DEBUG("bucket: " << offset);
    return offset;
}

size_t PackedMemoryArray::scan_node_any(int64_t* __restrict node, size_t node_sz, int64_t key) const noexcept {
    size_t offset = 0;
    while(offset < node_sz && node[offset] <= key){ offset++; }
    return offset;
}

size_t PackedMemoryArray::scan_node_min(int64_t* __restrict node, size_t node_sz, int64_t key) const noexcept {
    size_t offset = 0;
    while(offset < node_sz && node[offset] < key){ offset++; }
    return offset;
}

size_t PackedMemoryArray::scan_node_max(int64_t* __restrict node, size_t node_sz, int64_t key) const noexcept {
    if( key < node[0] )
        return 0;
    else {
        size_t offset = node_sz -1;
        while(node[offset] > key){ offset--; }
        return offset +1;
    }
}

int64_t PackedMemoryArray::find(int64_t key) const {
    if(empty()) return -1;

    auto segment_id = find_bucket_any(key);
    COUT_DEBUG("key: " << key << ", bucket: " << segment_id);
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
    return make_unique<pma::adaptive::int1::Iterator>(m_storage);
}

unique_ptr<::pma::Iterator> PackedMemoryArray::find(int64_t min, int64_t max) const {
    if(empty()) return empty_iterator();
    return make_unique<pma::adaptive::int1::Iterator> (m_storage, find_bucket_min(min), find_bucket_max(max), min, max );
}
unique_ptr<::pma::Iterator> PackedMemoryArray::iterator() const {
    if(empty()) return empty_iterator();
    return make_unique<pma::adaptive::int1::Iterator> (m_storage, 0, m_storage.m_number_segments -1,
            numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max()
    );
}

/*****************************************************************************
 *                                                                           *
 *   Sum                                                                     *
 *                                                                           *
 *****************************************************************************/

::pma::Interface::SumResult PackedMemoryArray::sum(int64_t min, int64_t max) const {
    return do_sum(m_storage, find_bucket_min(min), find_bucket_max(max), min, max );
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
void PackedMemoryArray::dump() const {
    dump(std::cout);
}

void PackedMemoryArray::dump(std::ostream& out) const {
    out << "[Settings] " << m_knobs << "\n";
    out << "[Index] block size: " << m_index.B << ", cardinality root: " << m_index.m_cardinality_root <<
            ", height: " << m_index.m_height << ", number of nodes (capacity): " << m_index.m_capacity <<
            ", offset leaves: " << m_index.m_offset_leaves << "\n";

    bool integrity_check = true;

    dump_node(out, 0, 0, 0, 1, &integrity_check);

    out << "\n";

    dump_storage(out, &integrity_check);

    out << "\n";

    dump_predictor(out);

    assert(integrity_check && "Integrity check failed!");
}

static void dump_tabs(std::ostream& out, size_t depth){
    using namespace std;

    auto flags = out.flags();
    out << setw((depth-1) * 2 + 5) << setfill(' ') << ' ';
    out.setf(flags);
}

void PackedMemoryArray::dump_node(std::ostream& out, size_t base, size_t offset, size_t num_nodes_in_this_level, int depth, bool* integrity_check) const {
    size_t N = depth == 1 ? m_index.m_cardinality_root : m_index.B -1;
    int64_t* keys = m_index.m_keys + base + offset;

    // preamble
    auto flags = out.flags();
    if(depth > 1) out << ' ';
    out << setw((depth -1) * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << "base: " << base << ", offset: " << offset << ", N: " << N << "\n";
    out.setf(flags);

    dump_tabs(out, depth);
    out << "keys: ";
    for(size_t i = 0; i < N; i++){
        if(i > 0) out << ", ";
        out << (i+1) << ": " << keys[i];
    }
    out << "\n";
    dump_tabs(out, depth);

    // internal node?
    if(depth < m_index.m_height) {

        size_t num_nodes_in_next_level = depth == 1 ? (N+1) : num_nodes_in_this_level * m_index.B;
        size_t base_next = (depth == 1 ? m_index.B : base + num_nodes_in_next_level);
        out << "base: " << base_next << ", ";
        out << "offsets: ";
        for(size_t i = 0; i < N +1; i++){
          if(i > 0) out << ", ";
          out << i << ": " << i * m_index.B;
        }
        out << endl;

        // recursively dump the children
        for(size_t i = 0; i < N +1; i++){
            dump_node(out, base_next, (offset + i) * m_index.B, num_nodes_in_next_level, depth +1, integrity_check);
        }

    } else { // leaf

        out << "offsets: ";
        for(size_t i = 0; i < N +1; i++){
            if(i > 0) out << ", ";
            size_t bucket_id = offset + i;
            out << i << ": " << bucket_id;

            int64_t* segment = m_storage.m_keys + bucket_id * m_storage.m_segment_capacity;

            if(bucket_id >= m_storage.m_number_segments){
                out << " (ERROR: invalid offset)";
                if(integrity_check) *integrity_check = false;
            } else if(i > 0){
                auto min_key = bucket_id % 2 == 0? segment[m_storage.m_segment_capacity - m_storage.m_segment_sizes[bucket_id]] : segment[0];
                if( keys[i-1] != min_key) {
                    out << " (ERROR: key mismatch in the storage: " << min_key << ")";
                    if(integrity_check) *integrity_check = false;
                }
            }
        }
        out << endl;
    }
}

void PackedMemoryArray::dump_storage(std::ostream& out, bool* integrity_check) const {
    cout << "[PMA] cardinality: " << m_storage.m_cardinality << ", capacity: " << m_storage.m_capacity << ", " <<
            "height: "<< m_storage.m_height << ", #segments: " << m_storage.m_number_segments <<
            ", blksz #elements: " << m_storage.m_segment_capacity << endl;

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

        if(keys[start] != get_pivot(i)){
            out << " (ERROR: invalid pivot, minimum: " << keys[start] << ", pivot: " << get_pivot(i) <<  ")" << endl;
            if(integrity_check) *integrity_check = false;
        }

        // next segment
        keys += m_storage.m_segment_capacity;
        values += m_storage.m_segment_capacity;
    }

    if(m_storage.m_cardinality != tot_count){
        out << " (ERROR: size mismatch, pma cardinality: " << m_storage.m_cardinality << ", sizes counted: " << tot_count <<  ")" << endl;
        if(integrity_check) *integrity_check = false;
    }
}

void PackedMemoryArray::dump_predictor(std::ostream& out) const  {
    m_detector.dump(out);
}



}}} // pma::adaptive::int1
