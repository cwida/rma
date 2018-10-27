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

#include "btreepma_v4.hpp"

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include "pma/generic/dynamic_index.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp"

using namespace pma::btree_pma_v4_detail;
using namespace std;

namespace pma {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[BTreePMA_v4::" << __FUNCTION__ << "] " << msg << std::endl
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
#define INDEX reinterpret_cast<DynamicIndex<int64_t, uint64_t>*>(m_index)
static constexpr int STORAGE_MIN_CAPACITY = 8; // the minimum capacity of the underlying PMA

BTreePMA_v4::BTreePMA_v4() : m_index(new DynamicIndex<int64_t, uint64_t>{}), m_storage(STORAGE_MIN_CAPACITY, /* fixed size segmnets ? */ false) {

}

BTreePMA_v4::BTreePMA_v4(uint64_t segment_capacity) : m_index(new DynamicIndex<int64_t, uint64_t>{}), m_storage(segment_capacity, /* fixed size segments ? */ true) {

}


BTreePMA_v4::~BTreePMA_v4() {
    delete INDEX; m_index = nullptr;
}

btree_pma_v4_detail::PMA::PMA(size_t segment_size, bool has_fixed_segment_capacity) : m_segment_capacity( hyperceil(segment_size ) ), m_has_fixed_segment_capacity(has_fixed_segment_capacity){
    if(hyperceil(segment_size ) > numeric_limits<uint16_t>::max()) throw std::invalid_argument("segment size too big, maximum is " + std::to_string( numeric_limits<uint16_t>::max() ));
    if(m_segment_capacity < 8) throw std::invalid_argument("segment size too small, minimum is 8");

    m_capacity = m_segment_capacity;
    m_number_segments = 1;
    m_height = 1;
    m_cardinality = 0;

    // memory allocations
    alloc_workspace(1, segment_size, &m_keys, &m_values, &m_segment_cardinalities);
}

btree_pma_v4_detail::PMA::~PMA(){
    free(m_keys); m_keys = nullptr;
    free(m_values); m_values = nullptr;
    free(m_segment_cardinalities); m_segment_cardinalities = nullptr;
}

void btree_pma_v4_detail::PMA::alloc_workspace(size_t num_segments, size_t segment_capacity, int64_t** keys, int64_t** values, decltype(m_segment_cardinalities)* cardinalities){
    // reset the ptrs
    *keys = nullptr;
    *values = nullptr;
    *cardinalities = nullptr;

    int rc(0);
    rc = posix_memalign((void**) keys, /* alignment */ 64,  /* size */ num_segments * segment_capacity * sizeof(m_keys[0]));
    if(rc != 0) {
        RAISE_EXCEPTION(Exception, "[PMA::PMA] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << num_segments * segment_capacity * sizeof(m_keys[0]));
    }
    rc = posix_memalign((void**) values, /* alignment */ 64,  /* size */ num_segments * segment_capacity * sizeof(m_values[0]));
    if(rc != 0) {
        free(*keys); *keys = nullptr;
        RAISE_EXCEPTION(Exception, "[PMA::PMA] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << num_segments * segment_capacity * sizeof(m_values[0]));
    }

    rc = posix_memalign((void**) cardinalities, /* alignment */ 64,  /* size */ num_segments * sizeof(m_segment_cardinalities[0]));
    if(rc != 0) {
        free(*keys); *keys = nullptr;
        free(*values); *values = nullptr;
        RAISE_EXCEPTION(Exception, "[PMA::PMA] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << num_segments * sizeof(m_segment_cardinalities[0]));
    }
}

/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/
size_t BTreePMA_v4::size() const {
    return m_storage.m_cardinality;
}

bool BTreePMA_v4::empty() const {
    return m_storage.m_cardinality == 0;
}

size_t BTreePMA_v4::segment_capacity() const{
    return m_storage.m_segment_capacity;
}

pair<double, double> BTreePMA_v4::thresholds(int height) {
    return thresholds(height, m_storage.m_height);
}

pair<double, double> BTreePMA_v4::thresholds(int node_height, int tree_height) {
    return m_density_bounds.thresholds(tree_height, node_height);
}

int64_t BTreePMA_v4::get_minimum(uint64_t segment_id) const{
    int64_t* __restrict keys = m_storage.m_keys;
    uint16_t* __restrict cardinalities = m_storage.m_segment_cardinalities;

    assert(segment_id < m_storage.m_number_segments && "Invalid segment");
    assert(cardinalities[segment_id] > 0 && "The segment is empty!");


    if(segment_id % 2 == 0){ // even segment
        return keys[(segment_id +1) * m_storage.m_segment_capacity - cardinalities[segment_id]];
    } else { // odd segment
        return keys[segment_id * m_storage.m_segment_capacity];
    }
}

uint64_t BTreePMA_v4::get_cardinality(uint64_t segment_id) const{
    assert(segment_id < m_storage.m_number_segments);
    return m_storage.m_segment_cardinalities[segment_id];
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void BTreePMA_v4::insert(int64_t key, int64_t value){
    COUT_DEBUG("key: " << key << ", value: " << value);

    if(empty()){
        insert_empty(key, value);
        INDEX->insert(key, 0); // point to segment 0
    } else {
        auto segment_id = index_find_leq(key);

        // is this segment full ?
        bool element_inserted_on_rebalance = false ;

        if( get_cardinality(segment_id) == m_storage.m_segment_capacity ){
            element_inserted_on_rebalance = rebalance(segment_id, key, value);

            if(!element_inserted_on_rebalance){ // resize
                segment_id = index_find_leq(key);
            }
        }

        if(!element_inserted_on_rebalance){
            int64_t pivot_old = get_minimum(segment_id);
            bool minimum_updated = insert_common(segment_id, key, value);

            // have we just updated the minimum ?
            if (minimum_updated){
                int64_t pivot_new = get_minimum(segment_id);
                index_update(pivot_old, pivot_new);
            }
        }
    }

#if defined(DEBUG)
    dump();
#endif
}

void BTreePMA_v4::insert_empty(int64_t key, int64_t value){
    assert(empty());
    assert(m_storage.m_capacity > 0 && "The storage does not have any capacity?");

    m_storage.m_segment_cardinalities[0] = 1;
    size_t offset = m_storage.m_segment_capacity -1;
    m_storage.m_keys[offset] = key;
    m_storage.m_values[offset] = value;
    m_storage.m_cardinality = 1;
}

bool BTreePMA_v4::insert_common(uint64_t segment_id, int64_t key, int64_t value){
    assert(m_storage.m_segment_cardinalities[segment_id] < m_storage.m_segment_capacity && "This segment is full!");

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    bool minimum = false; // the inserted key is the new minimum ?
    size_t sz = m_storage.m_segment_cardinalities[segment_id];

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
    }

    // update the cardinality
    m_storage.m_segment_cardinalities[segment_id]++;
    m_storage.m_cardinality += 1;

    return minimum;
}

// Change the separator key for a given entry in the index
void BTreePMA_v4::index_update(int64_t key_old, int64_t key_new){
    uint64_t segment_id =0;
    INDEX->remove_any(key_old, &segment_id);
    INDEX->insert(key_new, segment_id);
}

/*****************************************************************************
 *                                                                           *
 *   Rebalance                                                               *
 *                                                                           *
 *****************************************************************************/

bool BTreePMA_v4::rebalance(uint64_t segment_id, int64_t new_key, int64_t new_value){
    COUT_DEBUG("segment_id: " << segment_id);
    size_t num_elements = m_storage.m_segment_capacity +1;
    // these inits are only valid for the edge case that the calibrator tree has height 1, i.e. the data structure contains only one segment
    double rho = 0.0, theta = 1.0, density = static_cast<double>(num_elements)/m_storage.m_segment_capacity;
    size_t height = 1;
    COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta << ", num_elements: " << num_elements);

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
                num_elements += get_cardinality(index_left);
                index_left--;
            }
            while(index_right < window_end){
                num_elements += get_cardinality(index_right);
                index_right++;
            }

            COUT_DEBUG("num_elements: " << num_elements << ", window_start: " << window_start << ",  window_length: " << window_length << ",  segment_capacity: " << m_storage.m_segment_capacity);
            density = ((double) num_elements) / (window_length * m_storage.m_segment_capacity);

            COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta);
        } while (density > theta  && height < m_storage.m_height);
    }

    if(density <= theta){
        COUT_DEBUG("--SPREAD--");
        spread_insert spread_insert { new_key, new_value, segment_id };
        spread(num_elements, window_start, window_length, &spread_insert);
        return true;
    } else {
        COUT_DEBUG("--RESIZE--");
        resize();
        return false;
    }
}

void BTreePMA_v4::spread(size_t cardinality_after, size_t window_start, size_t num_segments, spread_insert* spread_insertion){
    int64_t insert_segment_id = spread_insertion != nullptr ? static_cast<int64_t>(spread_insertion->m_segment_id) - window_start : -1;
    COUT_DEBUG("size: " << cardinality_after << ", start: " << window_start << ", length: " << num_segments << ", insertion segment: " << insert_segment_id);
    assert(window_start % 2 == 0 && "Expected to start from an even segment");
    assert(num_segments % 2 == 0 && "Expected an even number of segments");

    // remove all keys related to this window from the index
    for(size_t i = 0; i < num_segments; i++){
        INDEX->remove_any(get_minimum(window_start +i));
    }

    // workspace
    using segment_card_t = remove_pointer_t<decltype(m_storage.m_segment_cardinalities)>;
    segment_card_t* __restrict cardinalities = m_storage.m_segment_cardinalities + window_start;
    int64_t* __restrict output_keys = m_storage.m_keys + window_start * m_storage.m_segment_capacity;
    int64_t* __restrict output_values = m_storage.m_values + window_start * m_storage.m_segment_capacity;

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
        int64_t output_segment_id = num_segments -2;
        int64_t output_start = (output_segment_id +1) * m_storage.m_segment_capacity - cardinalities[output_segment_id];
        int64_t output_end = output_start + cardinalities[output_segment_id] + cardinalities[output_segment_id +1];

        // copy the last four segments into input_chunk2_capacity
        int input_chunk2_segments_copied = 0;
        size_t input_chunk2_space_left = input_chunk2_capacity;
        while(output_segment_id >= 0 && input_chunk2_segments_copied < 4){
            size_t elements2copy = output_end - output_start;
            COUT_DEBUG("input_chunk2_segments_copied: " << input_chunk2_segments_copied << ", input_chunk2_space_left: " << input_chunk2_space_left << ", output_segment_id: " << output_segment_id << ", elements2copy: " << elements2copy);
            if(insert_segment_id == output_segment_id || insert_segment_id == output_segment_id +1){
                spread_insert_unsafe(output_keys + output_start, output_values + output_start,
                        input_chunk2_keys + input_chunk2_space_left - elements2copy -1, input_chunk2_values + input_chunk2_space_left - elements2copy -1,
                        elements2copy, spread_insertion->m_key, spread_insertion->m_value);
                input_chunk2_space_left--;
            } else {
                memcpy(input_chunk2_keys + input_chunk2_space_left - elements2copy, output_keys + output_start, elements2copy * sizeof(input_chunk2_keys[0]));
                memcpy(input_chunk2_values + input_chunk2_space_left - elements2copy, output_values + output_start, elements2copy * sizeof(input_chunk2_values[0]));
            }
            input_chunk2_space_left -= elements2copy;

            // fetch the next chunk
            output_segment_id -= 2;
            if(output_segment_id >= 0){
                output_start = (output_segment_id +1) * m_storage.m_segment_capacity - cardinalities[output_segment_id];
                output_end = output_start + cardinalities[output_segment_id] + cardinalities[output_segment_id +1];
            }

            input_chunk2_segments_copied += 2;
        }

        // readjust the pointers for input_chunk2
        input_chunk2_keys += input_chunk2_space_left;
        input_chunk2_values += input_chunk2_space_left;
        input_chunk2_size = input_chunk2_capacity - input_chunk2_space_left;

        // move the remaining elements towards the end of the array
        int64_t input_chunk1_current = num_segments * m_storage.m_segment_capacity;
        while(output_segment_id >= 0){
            size_t elements2copy = output_end - output_start;
            if(insert_segment_id == output_segment_id || insert_segment_id == output_segment_id +1){
                spread_insert_unsafe(output_keys + output_start, output_values + output_start,
                        output_keys + input_chunk1_current - elements2copy -1, output_values + input_chunk1_current - elements2copy -1,
                        elements2copy, spread_insertion->m_key, spread_insertion->m_value);
                input_chunk1_current--;
            } else {
                memcpy(output_keys + input_chunk1_current - elements2copy, output_keys + output_start, elements2copy * sizeof(output_keys[0]));
                memcpy(output_values + input_chunk1_current - elements2copy, output_values + output_start, elements2copy * sizeof(output_values[0]));
            }
            input_chunk1_current -= elements2copy;

            // fetch the next chunk
            output_segment_id -= 2;
            if(output_segment_id >= 0){
                output_start = (output_segment_id +1) * m_storage.m_segment_capacity - cardinalities[output_segment_id];
                output_end = output_start + cardinalities[output_segment_id] + cardinalities[output_segment_id +1];
            }
        }

        // readjust the pointers for input_chunk1
        input_chunk1_size = num_segments * m_storage.m_segment_capacity - input_chunk1_current;
        input_chunk1_keys = output_keys + input_chunk1_current;
        input_chunk1_values = output_values + input_chunk1_current;
    }

    // debug only
#if defined(DEBUG)
    size_t k = 0;
    for(size_t i = 0; i < input_chunk1_size; i++){
        cout << "Chunk 1 [" << k++ << "] <" << input_chunk1_keys[i] << ", " << input_chunk1_values[i] << ">\n";
    }
    for(size_t i = 0; i < input_chunk2_size; i++){
        cout << "Chunk 2 [" << k++ << "] <" << input_chunk2_keys[i] << ", " << input_chunk2_values[i] << ">\n";
    }
    std::flush(cout);
#endif

    // 2) set the expected size of each segment
    const size_t elements_per_segment = cardinality_after / num_segments;
    const size_t num_odd_segments = cardinality_after % num_segments;
    for(size_t i = 0; i < num_segments; i++){
        cardinalities[i] = elements_per_segment + (i < num_odd_segments);
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
    COUT_DEBUG("cardinality: " << cardinality_after << ", chunk1 size: " << input_chunk1_size << ", chunk2 size: " << input_chunk2_size);
    for(size_t i = 0; i < num_segments; i+=2){
        const size_t output_start = (i +1) * m_storage.m_segment_capacity - cardinalities[i];
        const size_t output_end = output_start + cardinalities[i] + cardinalities[i+1];
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
    }

    // update the tree
    for(size_t i = 0; i < num_segments; i++){
        INDEX->insert(get_minimum(window_start + i), window_start + i);
    }
}

void BTreePMA_v4::spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value){
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

void BTreePMA_v4::resize() {
    // compute the new capacity
    size_t cardinality = m_storage.m_cardinality;
    size_t capacity = m_storage.m_capacity, segment_capacity, num_segments, elements_per_segment, odd_segments;
    do {
        capacity *= 2;
        segment_capacity = m_storage.m_has_fixed_segment_capacity ? m_storage.m_segment_capacity : std::max<size_t>(hyperceil(log2(capacity)), STORAGE_MIN_CAPACITY);
        num_segments = capacity / segment_capacity;
        elements_per_segment = cardinality / num_segments;
        odd_segments = cardinality % num_segments;
    } while (elements_per_segment + (odd_segments > 0) == segment_capacity ); // avoid having segments full after a resize

    COUT_DEBUG(m_storage.m_capacity << " --> " << capacity << ", segment_capacity: " << segment_capacity << ", num_segments: " << num_segments << ", cardinality: " << cardinality);

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_cardinalities) ixSizes;
    PMA::alloc_workspace(num_segments, segment_capacity, &ixKeys, &ixValues, &ixSizes);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_cardinalities);
    auto xFreePtr = [](void* ptr){ free(ptr); };
    unique_ptr<int64_t, decltype(xFreePtr)> ixKeys_ptr { ixKeys, xFreePtr };
    unique_ptr<int64_t, decltype(xFreePtr)> ixValues_ptr{ ixValues, xFreePtr };
    unique_ptr<remove_pointer_t<decltype(m_storage.m_segment_cardinalities)>, decltype(xFreePtr)> ixSizes_ptr{ ixSizes, xFreePtr };
    int64_t* __restrict xKeys = m_storage.m_keys;
    int64_t* __restrict xValues = m_storage.m_values;
    decltype(m_storage.m_segment_cardinalities) __restrict xSizes = m_storage.m_segment_cardinalities;

    // fetch the first non-empty input segment
    size_t input_segment_id = 0;
    size_t input_size = ixSizes[0];
    assert(input_size > 0 && "There should be no empty segments, this impl~ does not support deletions");
    int64_t* input_keys = ixKeys + m_storage.m_segment_capacity - input_size;
    int64_t* input_values = ixValues + m_storage.m_segment_capacity - input_size;
    bool input_segment_odd = false; // consider '0' as even

    // start copying the elements
    bool output_segment_odd = false; // consider '0' as even
    for(size_t j = 0; j < num_segments; j++){
        // copy `elements_per_segment' elements at the start
        size_t elements_to_copy = elements_per_segment;
        if ( j < odd_segments ) elements_to_copy++;
        COUT_DEBUG("j: " << j << ", elements_to_copy: " << elements_to_copy);

        size_t output_offset = output_segment_odd ? 0 : segment_capacity - elements_to_copy;
        size_t output_canonical_index = j * segment_capacity;
        int64_t* output_keys = xKeys + output_canonical_index + output_offset;
        int64_t* output_values = xValues + output_canonical_index + output_offset;
        xSizes[j] = elements_to_copy;

        do {
            assert(elements_to_copy < segment_capacity && "Always keep at least 1 slot free for a future insertion");

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
                    assert(input_size > 0 && "There should be no empty segments, this impl~ does not support deletions");
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
    }

    // update the PMA properties
    m_storage.m_capacity = capacity;
    m_storage.m_segment_capacity = segment_capacity;
    m_storage.m_number_segments = num_segments;
    m_storage.m_height = log2(num_segments) +1;

    // rebuild the index
    INDEX->clear();
    for(size_t i = 0; i < num_segments; i++){
        INDEX->insert(get_minimum(i), i);
    }

    // side effect: regenerate the thresholds
    thresholds(m_storage.m_height, m_storage.m_height);
}

/*****************************************************************************
 *                                                                           *
 *   Lookup                                                                  *
 *                                                                           *
 *****************************************************************************/
uint64_t BTreePMA_v4::index_find_leq(int64_t key) const {
    uint64_t value = 0;
    bool found = INDEX->find_first(key, nullptr, &value);
    if(found){
        return value;
    } else {
        return 0;
    }
}

uint64_t BTreePMA_v4::index_find_geq(int64_t key) const {
    uint64_t value = 0;
    bool found = INDEX->find_last(key, nullptr, &value);
    if(found){
        return value;
    } else {
        return 0;
    }
}


int64_t BTreePMA_v4::find(int64_t key) const {
    if(empty()) return -1;
    auto segment_id = index_find_leq(key);

    COUT_DEBUG("key: " << key << ", segment: " << segment_id);

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_cardinalities[segment_id];
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
namespace btree_pma_v4_detail {

Iterator::Iterator(const PMA& storage) : m_pma(storage) { } // empty iterator

Iterator::Iterator(const PMA& storage, size_t segment_start, size_t segment_end, int64_t key_min /* incl */, int64_t key_max /* incl */) : m_pma(storage){
    if(segment_start > segment_end) throw invalid_argument("segment_start > segment_end");
    COUT_DEBUG("segment_start: " << segment_start << ", segment_end: " << segment_end << ", key_min: " << key_min << ", key_max: " << key_max);
    if(segment_end >= storage.m_number_segments) return;
    int64_t* __restrict keys = storage.m_keys;

    bool notfound = true;
    ssize_t segment_id = segment_start;
    bool segment_even = segment_id % 2 == 0;
    ssize_t start, stop = -1, offset = -1;

    while(notfound && segment_id < storage.m_number_segments){
        if(segment_even){
            stop = (segment_id +1) * storage.m_segment_capacity;
            start = stop - storage.m_segment_cardinalities[segment_id];
            COUT_DEBUG("lower interval, even segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
        } else { // odd
            start = segment_id * storage.m_segment_capacity;
            stop = start + storage.m_segment_cardinalities[segment_id];
            COUT_DEBUG("lower interval, odd segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
        }
        offset = start;

        while(offset < stop && keys[offset] < key_min) {
            COUT_DEBUG("lower interval, offset: " << offset << ", key: " << keys[offset] << ", key_min: " << key_min);
            offset++;

        }

        notfound = (offset == stop);
        if(notfound){
            segment_id++;
            segment_even = !segment_even; // flip
        }
    }

    m_offset = offset;
    m_next_segment = segment_id +1;
    m_stop = stop;
    if(segment_even && m_next_segment < storage.m_number_segments){
        m_stop = m_next_segment * storage.m_segment_capacity + storage.m_segment_cardinalities[m_next_segment];
        m_next_segment++;
    }

    if(notfound || keys[m_offset] > key_max){
        COUT_DEBUG("#1 - no qualifying interval for min");
        m_index_max = m_stop = 0;
    } else {
        // find the last qualifying index
        assert(segment_end < storage.m_number_segments);
        auto interval_start_segment = segment_id;
        segment_id = segment_end;
        segment_even = segment_id % 2 == 0;
        notfound = true;

        while(notfound && segment_id >= interval_start_segment){
            if(segment_even){
                start = (segment_id +1) * storage.m_segment_capacity -1;
                stop = start - storage.m_segment_cardinalities[segment_id];
            } else { // odd
                stop = segment_id * storage.m_segment_capacity;
                start = stop + storage.m_segment_cardinalities[segment_id] -1;
            }
            COUT_DEBUG("upper interval, " << (segment_even ? "even":"odd") << " segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
            offset = start;

            while(offset >= stop && keys[offset] > key_max) {
                COUT_DEBUG("upper interval, offset: " << offset << ", key: " << keys[offset] << ", key_max: " << key_max);
                offset--;
            }

            notfound = offset < stop;
            if(notfound){
                segment_id--;
                segment_even = !segment_even; // flip
            }
        }

        if(offset < static_cast<ssize_t>(m_offset)){
            COUT_DEBUG("#2 - no elements qualify for the interval [" << key_min << ", " << key_max << "]");
            m_index_max = m_stop = 0;
        } else {
            m_index_max = offset+1;
            m_stop = min(m_index_max, m_stop);

            COUT_DEBUG("m_start: " << m_offset << ", m_stop: " << m_stop << ", m_index_max: " << m_index_max);
        }
    }
}

void Iterator::next_sequence() {
    assert(m_offset >= m_stop);
    size_t segment1 = m_next_segment;

    if(segment1 < m_pma.m_number_segments){
        bool segment_even = segment1 % 2 == 0;
        if(segment_even){
            m_offset = segment1 * m_pma.m_segment_capacity + m_pma.m_segment_capacity - m_pma.m_segment_cardinalities[segment1];
            auto segment2 = segment1 +1;
            m_stop = segment2 * m_pma.m_segment_capacity;
            if(segment2 < m_pma.m_number_segments){
                m_stop = min(m_stop + m_pma.m_segment_cardinalities[segment2], m_index_max);
            } else {
                m_stop = min(m_stop, m_index_max);
            }

            m_next_segment += 2;
        } else { // odd segment
            m_offset = segment1 * m_pma.m_segment_capacity;
            m_stop = min(m_index_max, m_offset + m_pma.m_segment_cardinalities[segment1]);
            m_next_segment++;
        }
    }
}

bool Iterator::hasNext() const {
    return m_offset < m_stop;
}

std::pair<int64_t, int64_t> Iterator::next() {
    int64_t* keys = m_pma.m_keys;
    int64_t* values = m_pma.m_values;

    pair<int64_t, int64_t> result { keys[m_offset], values[m_offset] };

    m_offset++;
    if(m_offset >= m_stop) next_sequence();

    return result;
}

} // namespace btree_pma_v4_detail

std::unique_ptr<pma::Iterator> BTreePMA_v4::empty_iterator() const{
    return make_unique<btree_pma_v4_detail::Iterator>(m_storage);
}

std::unique_ptr<pma::Iterator> BTreePMA_v4::find(int64_t min, int64_t max) const {
    if((min > max) || empty()){ return empty_iterator(); }
    auto segment_min = index_find_leq(min);
    auto segment_max = index_find_geq(max);

    return make_unique<btree_pma_v4_detail::Iterator>(m_storage, segment_min, segment_max, min, max );
}

std::unique_ptr<pma::Iterator> BTreePMA_v4::iterator() const {
    return find(numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());
}

/*****************************************************************************
 *                                                                           *
 *   Aggregate sum                                                           *
 *                                                                           *
 *****************************************************************************/
pma::Interface::SumResult BTreePMA_v4::sum(int64_t min, int64_t max) const {
    if((min > max) || empty()){ return SumResult{}; }
    int64_t segment_start = index_find_leq(min);
    int64_t segment_end = index_find_geq(max);
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
            start = stop - m_storage.m_segment_cardinalities[segment_id];
            COUT_DEBUG("lower interval, even segment, start: " << start << ", stop: " << stop);
        } else { // odd
            start = segment_id * m_storage.m_segment_capacity;
            stop = start + m_storage.m_segment_cardinalities[segment_id];
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
        stop = (segment_id +1) * m_storage.m_segment_capacity + m_storage.m_segment_cardinalities[segment_id +1]; // +1 implicit
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
                stop = start - m_storage.m_segment_cardinalities[segment_id];
            } else { // odd
                stop = segment_id * m_storage.m_segment_capacity;
                start = stop + m_storage.m_segment_cardinalities[segment_id] -1;
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
            ssize_t size_lhs = m_storage.m_segment_cardinalities[segment_id];
            ssize_t size_rhs = m_storage.m_segment_cardinalities[segment_id +1];
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
void BTreePMA_v4::dump() const {
    dump(std::cout);
}

void BTreePMA_v4::dump(std::ostream& out) const {
    bool integrity_check = true;

    INDEX->dump(out);

    out << "\n";

    dump_storage(out, &integrity_check);

    assert(integrity_check && "Integrity check failed!");
}


void BTreePMA_v4::dump_storage(std::ostream& out, bool* integrity_check) const {
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
    auto sizes = m_storage.m_segment_cardinalities;
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

        // next segment
        keys += m_storage.m_segment_capacity;
        values += m_storage.m_segment_capacity;
    }

    if(m_storage.m_cardinality != tot_count){
        out << " (ERROR: size mismatch, pma registered cardinality: " << m_storage.m_cardinality << ", computed cardinality: " << tot_count <<  ")" << endl;
        if(integrity_check) *integrity_check = false;
    }
}

} /* namespace pma */
