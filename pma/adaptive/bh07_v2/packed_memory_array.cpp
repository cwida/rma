/*
 * packed_memory_array.cpp
 *
 *  Created on: Jul 10, 2018
 *      Author: dleo@cwi.nl
 */
#include "packed_memory_array.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iomanip>
#include <iostream>

#include "adaptive_rebalancing.hpp"
#include "buffered_rewired_memory.hpp"
#include "configuration.hpp"
#include "database.hpp"
#include "iterator.hpp"
#include "miscellaneous.hpp"
#include "rewired_memory.hpp"
#include "spread_with_rewiring.hpp"
#include "sum.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace bh07_v2 {

/*****************************************************************************
 *                                                                           *
 *   Debug                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[APMA_BH07_v2::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Profiling                                                               *
 *                                                                           *
 *****************************************************************************/
#if defined(PROFILING)
#define PROFILER_INIT auto _profiler = m_rebalancing_profiler.profiler(true);
#define PROFILER_START(action) _profiler.action##_start()
#define PROFILER_STOP(action) _profiler.action##_stop()
#define PROFILER_SEARCH_START PROFILER_START(search);
#define PROFILER_SEARCH_STOP PROFILER_STOP(search);
#define PROFILER_APMA_START PROFILER_START(apma);
#define PROFILER_APMA_STOP PROFILER_STOP(apma);
#define PROFILER_SPREAD_START _profiler.spread_start(window_length);
#define PROFILER_SPREAD_STOP PROFILER_STOP(spread);
#define PROFILER_RESIZE_START _profiler.resize_start(m_storage.m_capacity, m_storage.m_capacity * 2);
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
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/
APMA_BH07_v2::APMA_BH07_v2(size_t btree_block_size, size_t pma_segment_size, size_t pages_per_extent, double predictor_scale, const DensityBounds& density_bounds) :
    m_index(btree_block_size),
    m_storage(pma_segment_size, pages_per_extent),
    m_predictor(max(4, (int) predictor_scale), 1),
    m_density_bounds(density_bounds),
    m_predictor_scale(predictor_scale){ }

APMA_BH07_v2::~APMA_BH07_v2() {
    if(m_segment_statistics) record_segment_statistics();

    // no need to explicitly free m_storage

#if defined(PROFILING)
    if(config().db() != nullptr)
        m_rebalancing_profiler.save_results();
#endif
}

/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/
size_t APMA_BH07_v2::size() const noexcept {
    return m_storage.m_cardinality;
}

bool APMA_BH07_v2::empty() const noexcept {
    return size() == 0;
}

pair<double, double> APMA_BH07_v2::thresholds(int height) {
    return thresholds(height, m_storage.m_height);
}

pair<double, double> APMA_BH07_v2::thresholds(int node_height, int tree_height) {
    return m_density_bounds.thresholds(tree_height, node_height);
}

CachedMemoryPool& APMA_BH07_v2::memory_pool() {
    return m_memory_pool;
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void APMA_BH07_v2::insert(int64_t key, int64_t value){
    if(UNLIKELY( empty() )){
        insert_empty(key, value);
    } else {
        size_t segment = m_index.find(key);
        insert_common(segment, key, value);
    }

#if defined(DEBUG)
    dump();
#endif
}

void APMA_BH07_v2::insert_empty(int64_t key, int64_t value){
    assert(empty());
    assert(m_storage.m_capacity > 0 && "The storage does not have any capacity?");

    m_index.set_separator_key(0, key);
    m_storage.m_segment_sizes[0] = 1;
    size_t pos = m_storage.m_segment_capacity -1;
    m_storage.m_keys[pos] = key;
    m_storage.m_values[pos] = value;
    m_storage.m_cardinality = 1;

    m_predictor.update(0 /* -1 */);
}

void APMA_BH07_v2::insert_common(size_t segment_id, int64_t key, int64_t value){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_capacity && "Overflow: attempting to access an invalid segment in the PMA");

    COUT_DEBUG("segment_id: " << segment_id << ", element: <" << key << ", " << value << ">");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        // attempt #1: it may fail in case resizing or rewiring
        bool element_inserted = rebalance(segment_id, &key, &value);
        assert(m_memory_pool.empty() && "Memory leak!");
        // attempt #2: try again!
        if(!element_inserted) insert(key, value);
    } else { // find a spot where to insert this element
        bool minimum_updated = storage_insert_unsafe(segment_id, key, value);

        // have we just updated the minimum ?
        if (minimum_updated) m_index.set_separator_key(segment_id, key);

        m_predictor.update(segment_id);
    }
}

bool APMA_BH07_v2::storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value){
    assert(m_storage.m_segment_sizes[segment_id] < m_storage.m_segment_capacity && "This segment is full!");

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    bool minimum = false; // the inserted key is the new minimum ?
    size_t sz = m_storage.m_segment_sizes[segment_id];

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
    m_storage.m_segment_sizes[segment_id]++;
    m_storage.m_cardinality += 1;

    return minimum;
}

/*****************************************************************************
 *                                                                           *
 *   Rebalancing                                                             *
 *                                                                           *
 *****************************************************************************/
bool APMA_BH07_v2::rebalance(size_t segment_id, int64_t* key, int64_t* value){
    assert(key && value && "In this data structure we always rebalance when inserting a new key. Deletes are not supported.");
    constexpr bool is_insert = true; // as above
    PROFILER_INIT

    PROFILER_SEARCH_START
    COUT_DEBUG("segment_id: " << segment_id);
    size_t num_elements_after = m_storage.m_segment_capacity +1; // as this is an insertion, count the new element
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
    const bool is_spread = density <= theta;
//    const bool is_resize = !is_spread; // of course, either one or the other!
    bool new_element_inserted;
    const bool use_rewiring = is_spread && (m_storage.m_memory_keys != nullptr) &&
            (window_length * m_storage.m_segment_capacity * sizeof(int64_t) >= m_storage.m_memory_keys->get_extent_size());
    const size_t num_elements_current =num_elements_after -1; /* -1: the new key has not been inserted yet */

    if(is_spread){
        // adaptive strategy
        PROFILER_APMA_START
        VectorOfPartitions partitions = apma_partitions(height, num_elements_after, window_start, window_length, false, true);
        PROFILER_APMA_STOP

        PROFILER_SPREAD_START
        if(use_rewiring){
            COUT_DEBUG("-- SPREAD REWIRING --");
            SpreadWithRewiring rewiring_instance{this, (size_t) window_start, (size_t) window_length, partitions};
            rewiring_instance.set_element_to_insert(*key, *value, segment_id);
            rewiring_instance.execute();
            new_element_inserted = true;
        } else {
            COUT_DEBUG("-- SPREAD TWO COPIES --");
            spread_insertion insert { *key, *value, segment_id }; // insert the new element during a call to ::spread
            spread_two_copies(partitions, num_elements_current, window_start, window_length, &insert);
            new_element_inserted = true;
        }
        PROFILER_SPREAD_STOP
    } else { // resize
        m_storage.m_height++; // implicitly regenerate the thresholds at the next call of thresholds()

        /**
         * When resizing, for the adaptive rebalancing, we assume the capacity of the PMA has already been
         * doubled and the height increased by 1. Furthermore, for the last two segments in the calibrator
         * tree we allocate the same number of elements.
         */
        PROFILER_APMA_START
        VectorOfPartitions partitions = apma_partitions(height +1,
                num_elements_current /* -1: the new key has not been inserted yet */,
                window_start, 2* window_length, true, false);
        PROFILER_APMA_STOP

        COUT_DEBUG("-- RESIZE --");
        PROFILER_RESIZE_START
        resize(partitions);
        PROFILER_RESIZE_STOP

        new_element_inserted = false;
    }

    return new_element_inserted;
}

VectorOfPartitions APMA_BH07_v2::apma_partitions(int height, size_t cardinality, size_t segment_start, size_t num_segments, bool resize, bool can_fill_segments){
    return AdaptiveRebalancing::partitions(this, height, cardinality, segment_start, num_segments, resize, can_fill_segments);
}

void APMA_BH07_v2::spread_two_copies(const VectorOfPartitions& partitions, size_t cardinality /* excl. new key */, size_t segment_start, size_t num_segments, spread_insertion* insert){
    COUT_DEBUG("start: " << segment_start << ", length: " << num_segments);

    // adjust the cardinality to include the element to be inserted
    if(insert != nullptr) cardinality++;

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

    // 2) copy the elements from input_keys to the final segments
    size_t segment_id = 0;
    for(size_t i = 0, sz = partitions.size(); i < sz; i++){
        assert(partitions[i].m_segments > 0);
        size_t length = partitions[i].m_cardinality;
        if(partitions[i].m_segments == 1){ // copy a single segment
            spread_save(segment_start + segment_id, input_keys, input_values, length);
        } else {
            spread_save(segment_start + segment_id, partitions[i].m_segments, input_keys, input_values, length);
        }

        input_keys += length;
        input_values += length;
        segment_id += partitions[i].m_segments;
    }
    assert(segment_id == num_segments && "Not all segments visited");
}

void APMA_BH07_v2::spread_load(size_t window_start, size_t window_length, int64_t* __restrict keys_to, int64_t* __restrict values_to, spread_insertion* insert){
    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    int64_t* __restrict workspace_keys = m_storage.m_keys + window_start * m_storage.m_segment_capacity;
    int64_t* __restrict workspace_values = m_storage.m_values + window_start * m_storage.m_segment_capacity;
    segment_size_t* __restrict workspace_sizes = m_storage.m_segment_sizes + window_start;

    int64_t new_key_segment = insert ? (insert->m_segment - window_start) : -1;

    for(int64_t i = 1; i < window_length; i+=2){
        size_t length = workspace_sizes[i -1] + workspace_sizes[i];
        size_t offset = (m_storage.m_segment_capacity * i) - workspace_sizes[i-1];
        int64_t* __restrict keys_from = workspace_keys + offset;
        int64_t* __restrict values_from = workspace_values + offset;

        // destination
        if (new_key_segment == i || new_key_segment == i -1){
            spread_insert_unsafe(insert, keys_to, keys_from, values_to, values_from, length);
            insert = nullptr;
            keys_to++; values_to++;
        } else {
            memcpy(keys_to, keys_from, sizeof(keys_to[0]) * length);
            memcpy(values_to, values_from, sizeof(values_from[0]) * length);
        }

        keys_to += length; values_to += length;
    }

    if(insert != nullptr){
        spread_insert_unsafe(insert, keys_to, nullptr, values_to, nullptr, 0);
        insert = nullptr;
    }
}

void APMA_BH07_v2::spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality){
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
}


void APMA_BH07_v2::spread_save(size_t segment_start, size_t segment_length, int64_t* keys_from, int64_t* values_from, size_t cardinality){
    int64_t* __restrict keys_to = m_storage.m_keys + segment_start * m_storage.m_segment_capacity;
    int64_t* __restrict values_to = m_storage.m_values + segment_start * m_storage.m_segment_capacity;
    uint16_t* __restrict segment_sizes = m_storage.m_segment_sizes + segment_start;

    auto card_per_segment = cardinality / segment_length;
    auto odd_segments = cardinality % segment_length;

    // 1) set the segment sizes
    for(size_t i = 0; i < odd_segments; i++){
        segment_sizes[i] = card_per_segment +1;
    }
    for(size_t i = odd_segments; i < segment_length; i++){
        segment_sizes[i] = card_per_segment;
    }

    // 2) copy the first segment if it's at an odd position
    if(segment_start %2 == 1){
        size_t this_card = card_per_segment + odd_segments;
        memcpy(keys_to, keys_from, sizeof(keys_to[0]) * this_card);
        memcpy(values_to, values_from, sizeof(values_to[0]) * this_card);
        m_index.set_separator_key(segment_start, keys_to[0]);

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

        m_index.set_separator_key(segment_start + i-1,  keys_from[0]);
        m_index.set_separator_key(segment_start + i,   (keys_from + card_left)[0]);

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

        m_index.set_separator_key(segment_start + segment_length -1,  keys_from[0]);

        memcpy(keys_to_start, keys_from, card_per_segment * sizeof(keys_to_start[0]));
        memcpy(values_to_start, values_from, card_per_segment * sizeof(values_from[0]));
    }
}

void APMA_BH07_v2::spread_insert_unsafe(spread_insertion* insert, int64_t* keys_to, int64_t* keys_from, int64_t* values_to, int64_t* values_from, int length){
    assert(insert != nullptr);
    COUT_DEBUG("segment: " << insert->m_segment << ", key: " << insert->m_key << ", length: " << length);

    int64_t new_key = insert->m_key;

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
}

void APMA_BH07_v2::resize(const VectorOfPartitions& partitions){
    // use rewiring?
    if(m_storage.m_memory_keys != nullptr && m_storage.m_number_segments * m_storage.m_segment_capacity * sizeof(int64_t) >= m_storage.m_memory_keys->get_extent_size()){
        resize_rewire(partitions);
    } else { // standard method
        resize_general(partitions);
    }

    // rebuild the predictor
    size_t predictor_size = m_storage.m_height * m_predictor_scale;
    m_predictor.resize(max(4, (int) predictor_size));
    m_predictor.set_max_count(m_storage.m_height);

    // side effect: regenerate the thresholds
    thresholds(m_storage.m_height, m_storage.m_height);
}

void APMA_BH07_v2::resize_rewire(const VectorOfPartitions& partitions){
    const size_t num_segments_before = m_storage.m_number_segments;
    const size_t num_segments_after = num_segments_before * 2;
    COUT_DEBUG("segments: " << num_segments_before << " -> " << num_segments_after);

    // 1) Extend the PMA
    m_storage.extend(num_segments_before);
    m_index.rebuild(num_segments_after);

    // 2) Spread
    SpreadWithRewiring spread(this, 0, num_segments_after, partitions);
    size_t start_position = (num_segments_before -1) * m_storage.m_segment_capacity + m_storage.m_segment_sizes[num_segments_before -1];
    spread.set_absolute_position(start_position, true);
    spread.execute();
}


void APMA_BH07_v2::resize_general(const VectorOfPartitions& partitions) {
    constexpr bool is_insert = true; // it is always due to an insert
    size_t capacity = is_insert ?  m_storage.m_capacity * 2 : m_storage.m_capacity / 2; // new capacity
    size_t num_segments = capacity / m_storage.m_segment_capacity;
    COUT_DEBUG(m_storage.m_capacity << " --> " << capacity << ", num_segments: " << num_segments);

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    BufferedRewiredMemory* ixRewiredMemoryKeys;
    BufferedRewiredMemory* ixRewiredMemoryValues;
    RewiredMemory* ixRewiredMemoryCardinalities;
    decltype(m_storage.m_segment_sizes) ixSizes;
    m_storage.alloc_workspace(num_segments, &ixKeys, &ixValues, &ixSizes, &ixRewiredMemoryKeys, &ixRewiredMemoryValues, &ixRewiredMemoryCardinalities);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_sizes);
    swap(ixRewiredMemoryKeys, m_storage.m_memory_keys);
    swap(ixRewiredMemoryValues, m_storage.m_memory_values);
    swap(ixRewiredMemoryCardinalities, m_storage.m_memory_sizes);
    auto xDeleter = [&](void*){ Storage::dealloc_workspace(&ixKeys, &ixValues, &ixSizes, &ixRewiredMemoryKeys, &ixRewiredMemoryValues, &ixRewiredMemoryCardinalities); };
    unique_ptr<APMA_BH07_v2, decltype(xDeleter)> ixCleanup { this, xDeleter };
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

    // it's not necessary to rebuild the thresholds, already done in ::rebalance
}

/*****************************************************************************
 *                                                                           *
 *   Search                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t APMA_BH07_v2::find(int64_t key) const {
    if(empty()) return -1;

    auto segment_id = m_index.find(key);
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
unique_ptr<::pma::Iterator> APMA_BH07_v2::empty_iterator() const{
    return make_unique<pma::adaptive::bh07_v2::Iterator>(m_storage);
}

unique_ptr<::pma::Iterator> APMA_BH07_v2::find(int64_t min, int64_t max) const {
    if(empty()) return empty_iterator();
    return make_unique<pma::adaptive::bh07_v2::Iterator> (m_storage, m_index.find_first(min), m_index.find_last(max), min, max );
}
unique_ptr<::pma::Iterator> APMA_BH07_v2::iterator() const {
    if(empty()) return empty_iterator();
    return make_unique<pma::adaptive::bh07_v2::Iterator> (m_storage, 0, m_storage.m_number_segments -1,
            numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max()
    );
}

/*****************************************************************************
 *                                                                           *
 *   Sum                                                                     *
 *                                                                           *
 *****************************************************************************/
::pma::Interface::SumResult APMA_BH07_v2::sum(int64_t min, int64_t max) const {
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

decltype(auto) APMA_BH07_v2::compute_segment_statistics() const {
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

void APMA_BH07_v2::record_segment_statistics() const {
    LOG_VERBOSE("[APMA_BH07_v2] Computing segment statistics...");

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


void APMA_BH07_v2::set_record_segment_statistics(bool value) {
    m_segment_statistics = value;
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void APMA_BH07_v2::dump() const {
    dump(std::cout);
}

void APMA_BH07_v2::dump(std::ostream& out) const {
    bool integrity_check = true;

    m_index.dump(out, &integrity_check);

    out << "\n";

    dump_storage(out, &integrity_check);

    out << "\n";

    dump_predictor(out, &integrity_check);

    assert(integrity_check && "Integrity check failed!");
}

void APMA_BH07_v2::dump_storage(std::ostream& out, bool* integrity_check) const {
    cout << "[PMA] cardinality: " << m_storage.m_cardinality << ", capacity: " << m_storage.m_capacity << ", " <<
            "height: "<< m_storage.m_height << ", #segments: " << m_storage.m_number_segments <<
            ", blksz #elements: " << m_storage.m_segment_capacity << ", pages per extent: " << m_storage.m_pages_per_extent << endl;

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
        out << " (ERROR: size mismatch, pma cardinality: " << m_storage.m_cardinality << ", sizes counted: " << tot_count <<  ")" << endl;
        if(integrity_check) *integrity_check = false;
    }
}

void APMA_BH07_v2::dump_predictor(std::ostream& out, bool* integrity_check) const  {
    (void) integrity_check;
    m_predictor.dump(out);
}



}}} // pma::adaptive::bh07_v2




