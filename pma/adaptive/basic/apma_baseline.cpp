/*
 * apma_baseline.cpp
 *
 *  Created on: Jun 27, 2018
 *      Author: dleo@cwi.nl
 */

#include "apma_baseline.hpp"

#include <cmath>
#include <iomanip>
#include <iostream>

#include "errorhandling.hpp"
#include "miscellaneous.hpp"

using namespace pma::apma_baseline_details;
using namespace std;

namespace pma {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Predictor                                                               *
 *                                                                           *
 *****************************************************************************/
namespace apma_baseline_details {
#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) std::cout << "[Predictor::" << __FUNCTION__ << "] " << msg << std::endl

Predictor::Predictor(size_t size, size_t max_count) : m_buffer(nullptr){
    set_max_count(max_count);
    if(size <= 1){ throw std::invalid_argument("Invalid size: <= 1"); }
    resize(size);
}

Predictor::~Predictor(){
    deallocate_buffer(m_buffer);
}

Item* Predictor::allocate_buffer(size_t capacity){
    Item* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment */ 64,  /* size */ capacity * sizeof(Item));
    if(rc != 0) {
        RAISE_EXCEPTION(Exception, "[Predictor::allocate_buffer] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << (capacity * sizeof(Item)) << " bytes, capacity " << capacity);
    }
    memset(ptr, 0, capacity * sizeof(Item));
    return ptr;
}

void Predictor::deallocate_buffer(Item*& buffer){
    free(buffer); buffer = nullptr;
}

void Predictor::set_max_count(size_t value){
    if( value < 1 || value > pow(2, 16)){
        throw std::invalid_argument("Invalid value for the max count");
    }
    m_count_max = value;
}

void Predictor::update(uint64_t pointer){
    // find the position of the key in the predictor
    int64_t position = get_position(pointer);
    COUT_DEBUG("pointer: " << pointer << ", buffer_position: " << position);
    if(position != -1){ // the key exists
        position = move_ahead(position); // move ahead in the queue
        auto& item = m_buffer[position];
        assert(item.m_pointer == pointer && "the key should point to the same element in the PMA");
        if(item.m_count < m_count_max){ // increase the count
            item.m_count++;
        } else { // otherwise if it already reached the max value, decrease the count of the element at the tail of the queue
            decrease_tail();
        }
    } else if ( full() ) { // the key does not exist, but the array is full
        decrease_tail();
    } else { // the key does not exist, insert at the head of the data structure
        insert_new_element(pointer);
    }
}

int64_t Predictor::get_position(int64_t pointer) const {
    if(empty()) {
        return -1;
    } else if ( m_head > m_tail ){
        COUT_DEBUG("[case 1] from the head: " << (m_head -1) << " -> " << m_tail);
        for(int64_t i = m_head -1, end = m_tail; i >= end; i--){
            if(m_buffer[i].m_pointer == pointer)
                return i;
        }
        return -1;
    } else { // m_head <= m_tail
        for(int64_t i = m_head -1; i >= 0; i--){
            if(m_buffer[i].m_pointer == pointer)
                return i;
        }
        for(int64_t i = m_capacity -1, end = m_tail; i >= end; i--){
            if(m_buffer[i].m_pointer == pointer)
                return i;
        }

        return -1;
    }
}

void Predictor::decrease_tail() {
    assert(!empty() && "The data structure is empty");
    assert(m_buffer[m_tail].m_count > 0
            && "Inconsistent state: the tail points to an element that should have been removed, i.e. its count is 0");

    auto& item = m_buffer[m_tail];
    item.m_count--;
    if( item.m_count == 0 ){ // remove the element from the queue ?
        m_tail++; // => (m_tail + 1) % m_capacity
        if(m_tail == m_capacity) { // reset the pointer back at the start of the array
            m_tail = 0;
        }
        if(m_tail == m_head){ // another of those handsome corner cases
            m_empty = true;
        }
    }

}

size_t Predictor::move_ahead(size_t item_position){
    assert(!empty() && "The data structure is empty");
    assert(item_position < m_capacity && "Overflow");
    assert(m_buffer[item_position].m_count > 0 && "The count for this item is empty");

    size_t front = (m_head == 0 ? m_capacity -1 : m_head -1);
    COUT_DEBUG("item_position: " << item_position << ", front: " << front);
    if(item_position == front) { // already at the front of the queue?
        return item_position; // no change
    } else {
        size_t next_position = item_position == m_capacity -1 ? 0 : item_position +1;
        std::swap(m_buffer[item_position], m_buffer[next_position]);
        return next_position;
    }
}

void Predictor::insert_new_element(uint64_t pointer){
    assert(!full() && "The data structure is full");
    assert(m_head < m_capacity && "Index out of bound");
    assert(m_buffer[m_head].m_count == 0 && "Front of the queue not empty!");

    m_buffer[m_head] = { pointer, 1 };

    // move the pointer ahead of one position
    m_head++;
    if(m_head == m_capacity) m_head = 0; // and back

    m_empty = false;
}

void Predictor::resize(size_t sz) {
    deallocate_buffer(m_buffer);
    m_buffer = allocate_buffer(sz);
    m_capacity = sz;
    m_tail = m_head = 0;
    m_empty = true;
}

vector<Item> Predictor::weighted_elements(uint64_t min, uint64_t max){
    vector<Item> items;
    if(empty()) return items;

    items.reserve(m_capacity);

    auto buffer = m_buffer;
    auto append_if = [buffer, &items, min, max](size_t index){
        auto& item = buffer[index];
        assert(item.m_count > 0 && "Accessing an invalid element");

        if(min <= item.m_pointer && item.m_pointer <= max){
            items.push_back( item );
        }
    };

    if( m_head > m_tail ) {
        for(size_t j = m_tail; j < m_head; j++){
            append_if(j);
        }
    } else { // m_head <= m_tail
        for(size_t j = m_tail; j < m_capacity; j++){
            append_if(j);
        }
        for(size_t j = 0; j < m_head; j++){
            append_if(j);
        }
    }

    std::sort(begin(items), end(items), [](const Item& i1, const Item& i2){
       return i1.m_pointer < i2.m_pointer;
    });

    return items;
}

bool Predictor::empty() const {
    return m_empty;
}

bool Predictor::full() const {
    return !empty() && m_tail == m_head;
}

size_t Predictor::size() const {
    if(m_empty)
        return 0;
    else if(m_head > m_tail)
        return m_head - m_tail;
    else // m_head <= m_tail
        return (m_capacity - m_tail) + m_head;
}

void Predictor::reset_ptr(size_t index, size_t pma_position) {
    m_buffer[index].m_pointer = pma_position;
}

vector<Item> Predictor::items(uint64_t min, uint64_t max){
    return weighted_elements(min, max);
}

void Predictor::dump(std::ostream& out) const {
    out << "[Predictor] capacity: " << m_capacity << ", tail: " << m_tail << ", head: " << m_head << ", max count: " << m_count_max << ", size: " << size() << endl;
    if(empty()){
        out << "<empty>" << '\n';
    } else if( m_head > m_tail ) {
        size_t i = 0;
        for(size_t j = m_tail; j < m_head; j++){
            out << "[" << i << "] <" << j << "> " << m_buffer[j] << "\n";
            i++;
        }
    } else { // m_head <= m_tail
        size_t i = 0;
        for(size_t j  = m_tail; j < m_capacity; j++){
            out << "[" << i << "] <" << j << "> "<< m_buffer[j] << '\n';
            i++;
        }
        for(size_t j = 0; j < m_head; j++){
            out << "[" << i << "] <" << j << "> " << m_buffer[j] << '\n';
            i++;
        }
    }

    flush(out);
}

void Predictor::dump() const {
    dump(cout);
}

std::ostream& operator <<(std::ostream& out, const Item& item) {
    out << "pointer: " << item.m_pointer << ", count: " << item.m_count;
    return out;
}

} //namespace apma_baseline_details


#undef COUT_DEBUG_FORCE
#define COUT_DEBUG_FORCE(msg) std::cout << "[APMA_Baseline::" << __FUNCTION__ << "] " << msg << std::endl


/*****************************************************************************
 *                                                                           *
 *   Initialization                                                          *
 *                                                                           *
 *****************************************************************************/

APMA_Baseline::APMA_Baseline() : APMA_Baseline(4, 4) { }

//m_predictor(max(4, (int) predictor_scale), 1),
//m_density_bounds(bounds),
//m_predictor_scale(predictor_scale)

APMA_Baseline::APMA_Baseline(size_t index_B, size_t storage_B, double predictor_scale) :
        index(index_initialize(index_B, storage_B)), storage(), predictor(max(4, (int) predictor_scale), 1), predictor_scale(predictor_scale){
    storage_initialize(storage_min_capacity);

    // check the predictor scale
    if(predictor_scale <= 0.0){
        RAISE_EXCEPTION(Exception, "Invalid value for the predictor_scale: " << predictor_scale << " <= 0.0");
    }
}

APMA_Baseline::APMA_Baseline(size_t index_A, size_t index_B, size_t storage_A, size_t storage_B, double predictor_scale) :
        index( index_initialize(index_A, index_B, index_A, index_B, storage_A, storage_B) ),
        storage(),
        predictor(max(4, (int) predictor_scale), 1), predictor_scale(predictor_scale) {
    storage_initialize(storage_min_capacity);

    // check the predictor scale
    if(predictor_scale <= 0.0){
        RAISE_EXCEPTION(Exception, "Invalid value for the predictor_scale: " << predictor_scale << " <= 0.0");
    }
}

APMA_Baseline::~APMA_Baseline() {
    delete_node(index.root);
    delete[] storage.m_elements;
}

BTree APMA_Baseline::index_initialize(size_t index_B, size_t storage_B){
    return index_initialize(index_B /2, index_B, index_B /2, index_B, storage_B /2 , storage_B);
}

BTree APMA_Baseline::index_initialize(size_t intnode_a, size_t intnode_b, size_t leaf_a, size_t leaf_b, size_t storage_a, size_t storage_b){
    if(intnode_a <= 1) throw std::invalid_argument("intnode_a <= 1");
    if(2 * intnode_a > intnode_b) throw std::invalid_argument("2* intnode_a > intnode_b");
    if(leaf_a <= 1) throw std::invalid_argument("leaf_a <= 1");
    if(2 * leaf_a > leaf_b) throw std::invalid_argument("2* leaf_a > leaf_b");
    if(storage_a <= 1) throw std::invalid_argument("storage_a <= 1");
    if(2 * storage_a > storage_b) throw std::invalid_argument("2* storage_a > storage_b");

    BTree storage{intnode_a, intnode_b, leaf_a, leaf_b, storage_a, storage_b, nullptr};
    storage.root = create_leaf();

    return storage;
}

void APMA_Baseline::storage_initialize(size_t capacity) {
    storage.m_capacity = hyperceil(capacity);
    storage.m_segment_capacity = storage.m_capacity;
    storage.m_height = 1; //  log2(storage.capacity / storage.segment_size) +1;
    storage.m_elements = new element_t[storage.m_capacity];
    memset(storage.m_elements, 0xFF, storage.m_capacity * sizeof(storage.m_elements[0]));
    storage.m_cardinality = 0;
}

size_t APMA_Baseline::size() const {
    return storage.m_cardinality;
}

bool APMA_Baseline::empty() const {
    return storage.m_cardinality == 0;
}

/*****************************************************************************
 *                                                                           *
 *   Adaptive rebalancing                                                    *
 *                                                                           *
 *****************************************************************************/

decltype(auto) APMA_Baseline::apma_partitions(int height, size_t cardinality, size_t segment_start, size_t num_segments){
    assert(cardinality > 0 && "Are we resizing an empty array?");

    // find the min and max of the required region
    size_t min = segment_start;
    size_t max = min + num_segments -1;

    auto weights = predictor.items(min, max);
    vector<uint16_t> partitions;
    partitions.resize(num_segments); /* num_segments is already *2 if resize == true */


#if defined(DEBUG)
    COUT_DEBUG("height: " << height << ", cardinality: " << cardinality << ", segment_start: " << segment_start << ", segment_length: " << num_segments);
    COUT_DEBUG("weights size: " << weights.size() << ", min: " << min << ", max: " << max);
    for(size_t i = 0; i < weights.size(); i++){
        cout << "[" << i << "] " << weights[i] << "\n";
    }
    flush(cout);
#endif

    apma_compute_rec(partitions, - static_cast<int64_t>(segment_start), height, cardinality, segment_start, num_segments, weights, 0, weights.size());

#if !defined(NDEBUG)
    COUT_DEBUG("partitions: " << partitions.size());
    { // restrict the scope
        size_t sum = 0;
        for(size_t i = 0; i < partitions.size(); i++){
            sum += partitions[i];
            COUT_DEBUG("[" << segment_start + i << "] " << partitions[i]);
        }
        assert(sum == cardinality);
    }
#endif

    return partitions;
}

void APMA_Baseline::apma_compute_rec(vector<uint16_t>& partitions, int64_t partitions_base_adjustment, int height, size_t cardinality, size_t segment_start, size_t num_segments, const vector<apma_baseline_details::Item>& weights, size_t weights_start, size_t weights_length){
    COUT_DEBUG("--- apma_compute_partitions_rec ---");
    COUT_DEBUG("segment_start: " << segment_start << ", segment_length: " << num_segments << ", cardinality: " << cardinality <<
            ", height: " << height << ", weights_start: " << weights_start << ", weights_length: " << weights_length);

#if !defined(NDEBUG) // sanity check
    {
        double wrho, wtheta;
        storage_thresholds(height, wrho, wtheta);
        size_t wcapacity = num_segments * storage.m_segment_capacity;
        assert(wcapacity * wrho <= cardinality);
        assert(cardinality <= wcapacity * wtheta);
    }
#endif

    if(num_segments == 1){ // base case
        COUT_DEBUG("base case, segment: " << segment_start << ", cardinality: " << cardinality);
        assert(cardinality <= storage.m_segment_capacity);
        int64_t index = partitions_base_adjustment + static_cast<int64_t>(segment_start);
        assert(index < partitions.size());
        partitions[index] = static_cast<uint16_t>(cardinality);
    } else if (weights_length == 0){ // even rebalancing
        COUT_DEBUG("even rebalancing");
        int64_t elements_per_partition = cardinality / num_segments;
        int64_t odd_segments = cardinality % num_segments; // +1 element
        int64_t index_odd = segment_start + odd_segments;
        int64_t segment_end = segment_start + num_segments;
        assert(partitions_base_adjustment + segment_end <= partitions.size());
        for(int64_t i = segment_start; i < segment_end; i++){
            partitions[partitions_base_adjustment + i] = static_cast<uint16_t>(elements_per_partition + ((i < index_odd) ? 1 : 0));
        }
    } else { // uneven rebalancing
        COUT_DEBUG("uneven rebalancing");

        int height_children = height -1;
        assert(height_children >= 1);
        double rho, theta;
        storage_thresholds(height_children, rho, theta);
        size_t num_segments_children = pow(2.0, height_children -1);
        size_t capacity_children = storage.m_segment_capacity * num_segments_children;
        // ensure that at least one slot per segment will remain free
        const size_t max_window_cardinality = capacity_children - num_segments_children;
        COUT_DEBUG("capacity_children: " << capacity_children << ", max cardinality for the children's window: " << max_window_cardinality);

        size_t size_min = ceil(rho * capacity_children);
        size_t size_max = min<size_t>(theta * capacity_children, max_window_cardinality);
        size_t fill_min = max<int64_t>(0, static_cast<int64_t>(cardinality) - size_max); // avoid a value less than 0
        size_t fill_max = cardinality - size_min;

        size_t index_start = storage.m_segment_capacity * segment_start; // inclusive
//        size_t index_end = index_start + num_segments * m_storage.m_segment_capacity; // exclusive

        size_t start = max(size_min, fill_min);
        size_t end = min(size_max, fill_max);
        COUT_DEBUG("size_min: " << size_min << ", size_max: " << size_max << ", fill_min: " << fill_min << ", fill_max: " << fill_max << ", start: " << start << ", end: " << end);

        double denom_left = capacity_children - start;
        double denom_right = capacity_children - (cardinality - start);

        COUT_DEBUG("starting denom_left: " << denom_left << ", denom_right: " << denom_right);

        size_t sum_num_left = 0;
        size_t sum_num_right = 0;

        size_t weights_pos = weights_start;
        size_t weights_end = weights_start + weights_length;

        size_t index_opt = index_start + start;
        size_t splitnum = start;

        while(weights_pos < weights_end && (weights[weights_pos].m_pointer * storage.m_segment_capacity) < index_opt) {
            sum_num_left += weights[weights_pos].m_count;
            weights_pos++;
        }
        size_t weights_next_index, weights_next_ptr;
        if(weights_pos < weights_end){
            weights_next_index = weights_pos;
            weights_next_ptr = (weights[weights_next_index].m_pointer * storage.m_segment_capacity) +1;
        } else {
            weights_next_index = 0;
            weights_next_ptr = numeric_limits<size_t>::max();
        }
        size_t weights_split =  weights_pos - weights_start;

        while(weights_pos < weights_end){
            sum_num_right += weights[weights_pos].m_count;
            weights_pos++;
        }
        COUT_DEBUG("starting weights_split: " << weights_split << ", sum left: " << sum_num_left << ", sum right: " << sum_num_right);

        double optvalue = abs( (((double) sum_num_left) / denom_left) - (((double) sum_num_right) / max(1.0, denom_right)) );
        COUT_DEBUG("[" << start << "] starting optvalue: " << optvalue);


        for(size_t i = start +1; i <= end; i++){
            denom_left--;
            denom_right++;
            size_t index_cur = index_start + i;
            if(index_cur == weights_next_ptr){
                size_t count = weights[weights_pos].m_count;
                sum_num_left += count;
                sum_num_right -= count;
                weights_next_index++;
                if(weights_next_index < weights_end){
                    weights_next_ptr = (weights[weights_next_index].m_pointer * storage.m_segment_capacity) +1;
                } else {
                    weights_next_ptr = numeric_limits<size_t>::max();
                }
            }

            double curvalue = abs( (((double) sum_num_left) / max(1.0, denom_left)) - (((double) sum_num_right) / max(1.0, denom_right)) );
            COUT_DEBUG("[" << i << "] curvalue: " << curvalue << ", num elements left: " << i << ", num elements right: " << cardinality - i);

            if(curvalue < optvalue){
                optvalue = curvalue;
                splitnum = i;
                index_opt = index_cur;
                weights_split = weights_next_index - weights_start;
            }

        }

        size_t cardinality_left = splitnum;
        size_t cardinality_right = cardinality - splitnum;

        COUT_DEBUG("index_opt: " << index_opt << ", optvalue: " << optvalue << ", weights_split: " << weights_split);
        COUT_DEBUG("num elements left: " << cardinality_left << ", num elements right: " << cardinality_right);

        // recursive step
        size_t segment_split = num_segments / 2;
        apma_compute_rec(partitions, partitions_base_adjustment, height -1, cardinality_left, segment_start, segment_split, weights, weights_start, weights_split);
        apma_compute_rec(partitions, partitions_base_adjustment, height -1, cardinality_right, segment_start + segment_split, num_segments - segment_split, weights, weights_split, weights_length - weights_split);
    }
}


/*****************************************************************************
 *                                                                           *
 *   Rebalance                                                               *
 *                                                                           *
 *****************************************************************************/
void APMA_Baseline::storage_thresholds(size_t height, double& out_a, double& out_b) const{
    assert(height > 0 && height <= storage.m_height);
    double diff = (((double) storage.m_height) - height) / storage.m_height;
    out_a = r_0 - 0.25 * diff;
    out_b = t_0 + 0.25 * diff;
}

void APMA_Baseline::storage_rebalance(Leaf* leaf, size_t index_leaf, size_t index_insert) {
    size_t segment_id = index_insert / storage.m_segment_capacity;
    if(!storage_is_segment_full(segment_id)){ return; } // no rebalance is necessary

    int height = 1;
    int window_length = 1;
    int window_id = segment_id;
    int window_start = segment_id, window_end;
    double density = 1.0, rho, theta = t_0;
    size_t num_elements = storage.m_segment_capacity;

    if(storage.m_height > 1){
        // find the bounds of this window
        int index_left = segment_id -1;
        int index_right = segment_id +1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;
            storage_thresholds(height, rho, theta);

            // find the number of elements in the interval
            while(index_left >= window_start){
                num_elements += storage_get_segment_cardinality(index_left);
                index_left--;
            }
            while(index_right < window_end){
                num_elements += storage_get_segment_cardinality(index_right);
                index_right++;
            }

            COUT_DEBUG("num_elements: " << num_elements << ", window_start: " << window_start << ",  window_length: " << window_length << ", segment_capacity: " << storage.m_segment_capacity);
            density = ((double) num_elements) / (window_length * storage.m_segment_capacity);

            COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta);
        } while ((density >= theta) && height < storage.m_height);
    }

    if(density >= theta){
        /* traditional resizing */
        storage_resize();
    } else {
        /* adaptive spreading */
        auto partitions = apma_partitions(height, num_elements, window_start, window_length);
        storage_spread(partitions, leaf, index_leaf, num_elements, window_start, window_length);
    }
}

void APMA_Baseline::storage_spread(const std::vector<uint16_t>& partitions, Leaf* leaf, size_t index_leaf, size_t num_elements, size_t window_start, size_t window_length){
    COUT_DEBUG("leaf: " << leaf << ", index_leaf: " << index_leaf << ", num_elements: " << num_elements << ", window_start: " << window_start << ", window_length: " << window_length);
    const size_t window_end = window_start + window_length;

    // create the workspace
    std::unique_ptr<element_t[]> workspace_ptr(new element_t[window_length * storage.m_segment_capacity]); // delete[] tmp* when it goes out of scope
    element_t* __restrict workspace = workspace_ptr.get();

    // find the first valid index for a leaf. We may need to check backward or ahead
    size_t index_next = window_start * storage.m_segment_capacity;
    size_t index_last = (window_end -1) * storage.m_segment_capacity + storage_get_segment_cardinality(window_end -1) -1;
    size_t value_leaf = -1;
    if(values(leaf)[index_leaf] < index_next){
        do {
            index_leaf++;
            if(index_leaf == leaf->N){
                leaf = leaf->next;
                index_leaf = 0;
            }
        } while(leaf && values(leaf)[index_leaf] < index_next);
        if(leaf && values(leaf)[index_leaf] <= index_last){
            value_leaf = values(leaf)[index_leaf];
        }
    } else {
        Leaf* leaf_prev = leaf;
        size_t index_leaf_prev = index_leaf;
        while(leaf_prev && values(leaf_prev)[index_leaf_prev] >= index_next){
            leaf = leaf_prev;
            index_leaf = index_leaf_prev;

            if(index_leaf_prev == 0){
                leaf_prev = leaf_prev->prev;
                if(leaf_prev) { index_leaf_prev = leaf_prev->N -1; }
            } else {
                index_leaf_prev--;
            }
        }
        // A ptr in the interval
        value_leaf = values(leaf)[index_leaf];
    }

    // copy the elements in the workspace
    size_t pos = 0; // current position in the workspace
    for(size_t i = 0; i < window_length; i++){
        size_t segment_cardinality = partitions[i];
        assert(segment_cardinality < storage.m_segment_capacity && "There should be at least one empty slot in each segment");

        // copy `segment_cardinality' elements
        for(size_t j = 0; j < segment_cardinality; j++){
            workspace[pos] = storage.m_elements[index_next];

            // update the pointer in the index
            if(value_leaf == index_next){
                values(leaf)[index_leaf] = window_start * storage.m_segment_capacity + pos;

                index_leaf++;
                if(index_leaf >= leaf->N){
                    leaf = leaf->next;
                    index_leaf = 0;
                }
                if(leaf) value_leaf = values(leaf)[index_leaf];
            }

            // move to the next position src PMA
            do { index_next++; } while (index_next < storage.m_capacity && storage.m_elements[index_next].key < 0);

            // move to the next position in the workspace / dest PMA
            pos++;
        }

        // set to -1 the remaining elements in the workspace array
        for(size_t j = 0, end = storage.m_segment_capacity - segment_cardinality; j < end; j++){
            workspace[pos].key = -1;
            pos++;
        }
    }

    // copy the elements back from the workspace to the PMA
    memcpy(storage.m_elements + window_start * storage.m_segment_capacity, workspace, window_length * storage.m_segment_capacity * sizeof(element_t));
}


void APMA_Baseline::storage_resize(){
    // compute the new capacity
    size_t capacity = storage.m_capacity * 2;
    COUT_DEBUG("new capacity: " << capacity);
    size_t segment_capacity = hyperceil(log2(capacity));
    size_t num_segments = capacity / segment_capacity;
    std::unique_ptr<element_t[]> elements_ptr{ new element_t[capacity] };
    auto* __restrict elements = elements_ptr.get();
    memset(elements, 0xFF, capacity * sizeof(elements[0])); // init all cells to < 0

    // find the leftmost entry in the index
    Node* node = index.root;
    while(!node->is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        node = children(inode)[0];
    }
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    size_t index_leaf = 0;
    size_t value_leaf = values(leaf)[index_leaf];

    size_t elements_per_segment = storage.m_cardinality / num_segments;
    size_t odd_segments = storage.m_cardinality % num_segments;
    COUT_DEBUG("elements per segment: " << elements_per_segment << ", odd segments: " << odd_segments << ", num segments: " << num_segments << ", segment capacity: " << segment_capacity);

    size_t inserted_elements = 0;
    assert(!storage_is_cell_empty(0) && "The first position should not be empty");
    size_t index_next = 0; // the next index to copy from the PMA being resized

    for(size_t i = 0; i < num_segments; i++){
        size_t segment_cardinality = elements_per_segment + (i < odd_segments);
        size_t current_index = i * segment_capacity;
        for(size_t j = 0; j < segment_cardinality; j++){
            // copy the element
            COUT_DEBUG("segment: " << i << ", item: " << j << ", copy from: " << index_next << ", to: " << current_index);
            elements[current_index] = storage.m_elements[index_next];

            // update the pointer in the index
            if(value_leaf == index_next){
                values(leaf)[index_leaf] = current_index;

                index_leaf++;
                if(index_leaf >= leaf->N){
                    leaf = leaf->next;
                    index_leaf = 0;
                }
                if(leaf) value_leaf = values(leaf)[index_leaf];
            }

            // next position to set
            current_index++;

            // next element to copy
            inserted_elements++;
            if(inserted_elements < storage.m_cardinality){
                do{ index_next++; } while ( storage.m_elements[index_next].key < 0 );
            }
        }
    }

    // clean up
    delete[] storage.m_elements; storage.m_elements = nullptr;
    storage.m_capacity = capacity;
    storage.m_segment_capacity = segment_capacity;
    storage.m_elements = elements; elements_ptr.release();
    storage.m_height = log2(storage.m_capacity / storage.m_segment_capacity) +1;

    // update the predictor
    size_t predictor_size = storage.m_height * predictor_scale;
    predictor.resize(max(4, (int) predictor_size));
    predictor.set_max_count(storage.m_height);
}

size_t APMA_Baseline::storage_get_num_segments() const {
    assert(storage.m_capacity % storage.m_segment_capacity == 0 && "The overall capacity should be a multiple of the segment capacity");
    return storage.m_capacity / storage.m_segment_capacity;
}

bool APMA_Baseline::storage_is_segment_full(size_t segment_id) const {
    assert(segment_id < storage_get_num_segments() && "Invalid index for the segment id, overflow");
    return !storage_is_cell_empty((1+segment_id) * storage.m_segment_capacity -1);
}

size_t APMA_Baseline::storage_get_segment_cardinality(size_t segment_id) const{
    assert(segment_id < storage_get_num_segments() && "Invalid index for the segment id, overflow");
    int64_t start = segment_id * storage.m_segment_capacity;
    int64_t pos = start + storage.m_segment_capacity - 1;
    assert(pos < storage.m_capacity && "Index out of bounds");
    while(pos > start && storage_is_cell_empty(pos)) pos--;
    return pos +1 - start;
}

/*****************************************************************************
 *                                                                           *
 *   Insertion                                                               *
 *                                                                           *
 *****************************************************************************/
void APMA_Baseline::index_split_root(){

    auto root0 = create_internal_node();
    children(root0)[0] = index.root;
    root0->N =1;
    index_split_node(root0, 0);
    index.root = root0;
}

void APMA_Baseline::index_split_node(InternalNode* inode, size_t child){
    assert(inode != nullptr);
    assert(child <= inode->N);

    int64_t pivot = -1;
    Node* ptr = nullptr;

    // split a leaf in half
    if(children(inode)[child]->is_leaf){
        Leaf* l1 = (Leaf*) children(inode)[child];
        Leaf* l2 = create_leaf();

        size_t thres = (l1->N) /2;
        l2->N = l1->N - (thres +1);
        assert(l2->N >= index.leaf_a);
        l1->N = thres +1;
        assert(l1->N >= index.leaf_a);

        // move the elements from l1 to l2
        memcpy(keys(l2), keys(l1) + thres +1, (l2->N -1) * sizeof(keys(l2)[0]));
        memcpy(values(l2), values(l1) + thres +1, l2->N * sizeof(values(l2)[0]));
        memcpy(cardinalities(l2), cardinalities(l1) + thres +1, l2->N * sizeof(cardinalities(l2)[0]));

        // adjust the links
        l2->next = l1->next;
        if(l2->next != nullptr){ l2->next->prev = l2; }
        l2->prev = l1;
        l1->next = l2;

        // threshold derives the new pivot
        pivot = keys(l1)[thres];
        ptr = l2;
    }

    // split an internal node
    else {
        InternalNode* n1 = (InternalNode*) children(inode)[child];
        InternalNode* n2 = create_internal_node();

        size_t thres = n1->N /2;
        n2->N = n1->N - (thres +1);
        assert(n2->N >= index.intnode_a);
        n1->N = thres +1;
        assert(n1->N >= index.intnode_a);

        // move the elements from n1 to n2
        assert(n2->N > 0);
        memcpy(keys(n2), keys(n1) + thres + 1, (n2->N -1) * sizeof(keys(n2)[0]));
        memcpy(children(n2), children(n1) + thres +1, (n2->N) * sizeof(children(n2)[0]));

        // derive the new pivot
        pivot = keys(n1)[thres];
        ptr = n2;
    }

    // finally, add the pivot to the parent (current node)
    assert(inode->N <= index.intnode_b); // when inserting, the parent is allowed to become b+1
    int64_t* inode_keys = keys(inode);
    Node** inode_children = children(inode);
    for(size_t i = inode->N -1; i > child; i--){
        inode_keys[i] = inode_keys[i-1];
        inode_children[i +1] = inode_children[i];
    }

    inode_keys[child] = pivot;
    inode_children[child+1] = ptr;
    inode->N++;
}

void APMA_Baseline::insert(int64_t key, int64_t value){
    COUT_DEBUG("key: " << key << ", value: " << value);

    // Insert the given key/value in the subtree rooted at the given `node'.
    if(UNLIKELY(index.root->is_leaf)){
        insert(reinterpret_cast<Leaf*>(index.root), key, value);
    } else {
        insert(reinterpret_cast<InternalNode*>(index.root), key, value);
    }

    // split the root of the index when it is is full
    // nodes are allowed to overflow of an element more, the node is split in post-order
    if( (!index.root->is_leaf && index.root->N > index.intnode_b) ||
            (index.root->is_leaf && index.root->N > index.leaf_b)){
        index_split_root();
    }
}

void APMA_Baseline::insert(Node* node, int64_t key, int64_t value){
    assert(node != nullptr);

    // tail recursion on the internal nodes
    while(!node->is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        assert(inode->N > 0);
        size_t i = 0, last_key = inode->N -1;
        int64_t* __restrict inode_keys = keys(inode);
        while(i < last_key && key > inode_keys[i]) i++;
        node = children(inode)[i];

        // before moving to its child, check whether it is full. If this is the case
        // we need to make a recursive call to check again whether we need to split the
        // node after the element was inserted
        if( node->is_leaf && node->N == index.leaf_b ) {
            insert(node, key, value);
            if(node->N > index.leaf_b) { index_split_node(inode, i); }
            return; // stop the loop
        }
        else if ( !node->is_leaf && node->N == index.intnode_b ){
            insert(node, key, value);
            if(node->N > index.intnode_b ) { index_split_node(inode, i); }
            return; // stop the loop
        }
    }

    // finally, propagate to the leaf
    insert_leaf(reinterpret_cast<Leaf*>(node), key, value);
}

void APMA_Baseline::insert_empty(int64_t key, int64_t value){
    assert(empty());
    COUT_DEBUG("key: " << key << ", value: " << value);

    Leaf* leaf = reinterpret_cast<Leaf*>(index.root);
    values(leaf)[0] = 0;
    cardinalities(leaf)[0] = 1;
    leaf->N = 1;
    assert(storage.m_capacity > 0 && "The storage does not have any capacity?");
    storage.m_elements[0] = element_t{key, value};
    storage.m_cardinality = 1;
}

void APMA_Baseline::insert_leaf(Leaf* leaf, int64_t key, int64_t value){
    assert(leaf != nullptr);
    assert(leaf->N <= index.leaf_b);
    COUT_DEBUG("leaf: " << leaf << ", key: " << key << ", value: " << value);

    // edge case: this container is empty!
    if(UNLIKELY(leaf->empty())){
        assert(index.root == leaf); // this must be the very first element in the container!
        insert_empty(key, value);
    }

    // regular case, find the position of the key in the leaf
    else {
        // find the position of the key in the leaf
        size_t i = 0;
        size_t N = leaf->N -1;
        int64_t* __restrict leaf_keys = keys(leaf);
        while(i < N && leaf_keys[i] < key) i++;
        insert_storage(leaf, i, key, value);

        // update the cardinality
        cardinalities(leaf)[i]++;

        // add a new element in the leaf ?
        if(cardinalities(leaf)[i] > index.storage_b){
            index_leaf_augment(leaf, i);
        }
    }
}

// Add a new element in the leaf
void APMA_Baseline::index_leaf_augment(Leaf* leaf, size_t pos){
    assert(cardinalities(leaf)[pos] > index.storage_b); // reminder

    int64_t* __restrict leaf_keys = keys(leaf);
    uint64_t* __restrict leaf_values = values(leaf);
    decltype(cardinalities(leaf)) __restrict leaf_card = cardinalities(leaf);

    // move the successive elements forward (make room)
    for(size_t i = leaf->N -1; i > pos; i--){
        leaf_values[i +1] = leaf_values[i];
        leaf_card[i +1] = leaf_card[i];
        leaf_keys[i] = leaf_keys[i-1];
    }

    // find the pivot in the storage
    size_t index_storage = leaf_values[pos];
    size_t num_elements = leaf_card[pos] /2;
    element_t* __restrict elements = storage.m_elements;
    for(size_t i = 0; i < num_elements; i++){
        do { index_storage++; } while(elements[index_storage].key < 0);
    }

    // add the pivot in the leaf
    leaf_keys[pos] = storage.m_elements[index_storage].key;
    leaf_values[pos +1] = index_storage;
    leaf_card[pos +1] = leaf_card[pos] - num_elements;
    leaf_card[pos] = num_elements;

    leaf->N++;
}

void APMA_Baseline::insert_storage(Leaf* leaf, size_t index_leaf, int64_t key, int64_t value){
    uint64_t* values_leaf = values(leaf);
    size_t pos = values_leaf[index_leaf];
    COUT_DEBUG("pointed position from the index: " << pos);

    // find the actual position where we can insert in the storage
    assert(storage.m_elements[pos].key >= 0 && "The index cannot point to an empty position in the storage!");
    while(pos < storage.m_capacity && key > storage.m_elements[pos].key){
        do { pos++; } while(pos < storage.m_capacity && storage.m_elements[pos].key < 0);
    }

    // the slot where we will insert the element
    size_t index_insert {0};

    // edge case, we are trying to insert a new maximum for the PMA
    if(pos == storage.m_capacity){
        size_t last_segment = storage_get_num_segments() -1;
        index_insert = last_segment * storage.m_segment_capacity + storage_get_segment_cardinality(last_segment);
        assert(storage_is_cell_empty(index_insert) && "We should have landed to an empty slot");
    } else { // find an empty slot on the right
        int64_t j = pos;
        int64_t capacity = storage.m_capacity;
        while(j < capacity && !storage_is_cell_empty(j)) j++;
        assert(static_cast<size_t>(j) < storage.m_capacity && "An empty slot on the right is always expected to be found!");
        storage_shift_right(leaf, index_leaf, pos, j - pos);
        index_insert = pos;

        assert(((index_insert / storage.m_segment_capacity) == (j / storage.m_segment_capacity)) &&
                "Expected to expand the same segment where the element has been inserted");
    }

    // insert the element
    storage.m_elements[index_insert] = element_t{key, value};
    storage_set_presence<true>(index_insert);
    storage.m_cardinality++;

    if(index_insert < values_leaf[index_leaf]){
        values_leaf[index_leaf] = index_insert;
    }

    // update the entry in the predictor
    predictor.update(/* segment_id = */ index_insert / storage.m_segment_capacity);

    // rebalance
    storage_rebalance(leaf, index_leaf, index_insert);
}

bool APMA_Baseline::storage_is_cell_empty(size_t pos) const{
    assert(pos < storage.m_capacity);
    return storage.m_elements[pos].key < 0;
}

// increase the cardinality of the segment containing the position `end'
void APMA_Baseline::storage_shift_right(Leaf* leaf, size_t index_leaf, size_t start, size_t length){
    size_t end = start + length; // the last position to be moved, at the end it should be present

    assert(storage_is_cell_empty(end));

    // move the elements forward
    for(size_t i = end; i > start; i--){
        storage.m_elements[i] = storage.m_elements[i-1];
    }

    storage_set_presence<false>(start);

    // update the index
    uint64_t* leaf_values = values(leaf);
    while(leaf && leaf_values[index_leaf] < end){
        if(leaf_values[index_leaf] >= start){
            leaf_values[index_leaf]++;
        }

        // next pointer
        index_leaf++;
        if(index_leaf == leaf->N){
            leaf = leaf->next;
            leaf_values = values(leaf);
            index_leaf = 0;
        }
    }
}

template<bool value>
void APMA_Baseline::storage_set_presence(size_t position){
    assert(position < storage.m_capacity);
    assert((value && storage.m_elements[position].key >= 0) || !value);

    if(!value){
        storage.m_elements[position].key = -1;
    }
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/
int64_t APMA_Baseline::find(int64_t key) const {
    if(empty()) return -1;

    Node* node = index.root;
    assert(node != nullptr);

    // percolate on the inner nodes
    while(!node->is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, sz = inode->N -1;
        int64_t* __restrict inode_keys = keys(inode);
        while(i < sz && inode_keys[i] < key) i++;
        if(i < sz && inode_keys[i] == key) i++;
        node = children(inode)[i];
    }

    // percolate on the leaf
    assert(node->is_leaf);
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    size_t i = 0, sz = leaf->N -1;
    int64_t* __restrict leaf_keys = keys(leaf);
    while(i < sz && leaf_keys[i] < key) i++;
    if(i < sz && leaf_keys[i] == key) i++;
    size_t pos = values(leaf)[i];

    // search in the PMA
    assert(!storage_is_cell_empty(pos));
    decltype(storage.m_elements) __restrict elements = storage.m_elements;
    const size_t capacity = storage.m_capacity;
    while(pos < capacity && (/*elements[pos].key < 0 ||*/ elements[pos].key < key)) pos++;
    if(pos < capacity && elements[pos].key == key){
        return storage.m_elements[pos].value;
    } else {
        return -1; /* not found */
    }
}

std::unique_ptr<::pma::Iterator> APMA_Baseline::find(int64_t min, int64_t max) const {
    if((min > max) || empty()){ return empty_iterator(); }

    // percolate the internal nodes
    Node* node = index.root;
    while(!node->is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, sz = inode->N -1;
        int64_t* __restrict inode_keys = keys(inode);
        while(i < sz && inode_keys[i] < min) i++;
        node = children(inode)[i];
    }

    // percolate the leaves
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0);
    size_t i = 0, sz = leaf->N -1;
    int64_t* __restrict leaf_keys = keys(leaf);
    while(i < sz && leaf_keys[i] < min) i++;

    // the first entry must be at an certain index greater or equal than pos
    // let the Iterator::ctor perform the final scan
    size_t pos = values(leaf)[i];
    return std::make_unique<apma_baseline_details::Iterator>(storage, pos, min, max);
}

/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/
namespace apma_baseline_details {
Iterator::Iterator(const PMA& storage, size_t start, int64_t key_min, int64_t key_max) :
        storage(storage), pos(start), key_max(key_max) {
    // find the first valid element
    if(key_min < 0) key_min = 0;
    size_t capacity = storage.m_capacity;
    while(pos < capacity && storage.m_elements[pos].key < key_min) pos++;
}

bool Iterator::hasNext() const{
    return pos < storage.m_capacity && storage.m_elements[pos].key >= 0 && storage.m_elements[pos].key <= key_max;
}

std::pair<int64_t, int64_t> Iterator::next(){
    assert(hasNext());
    element_t* __restrict elements = storage.m_elements;
    typedef std::pair<int64_t, int64_t> pair_t;
    static_assert(sizeof(pair_t) == sizeof(elements[pos]), "Size mismatch");
    pair_t result =  *reinterpret_cast<pair_t*>(elements + pos);

    // move to the next valid element
    do{ pos++; } while (pos < storage.m_capacity && elements[pos].key < 0);

    return result;
}
} // namespace apma_baseline_details

std::unique_ptr<::pma::Iterator> APMA_Baseline::iterator() const{
    return std::make_unique<apma_baseline_details::Iterator>(storage, 0,
            std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
}

std::unique_ptr<::pma::Iterator> APMA_Baseline::empty_iterator() const{
    return std::make_unique<apma_baseline_details::Iterator>(storage,
            /* starting position = */ storage.m_capacity, 0, 0);
}

/*****************************************************************************
 *                                                                           *
 *   Sum                                                                     *
 *                                                                           *
 *****************************************************************************/

::pma::Interface::SumResult APMA_Baseline::sum(int64_t min, int64_t max) const {
    if((min > max) || empty()){ return SumResult{}; }

    // percolate the internal nodes
    Node* node = index.root;
    while(!node->is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, sz = inode->N -1;
        int64_t* __restrict inode_keys = keys(inode);
        while(i < sz && inode_keys[i] < min) i++;
        node = children(inode)[i];
    }

    // percolate the leaves
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0);
    size_t i = 0, sz = leaf->N -1;
    int64_t* __restrict leaf_keys = keys(leaf);
    while(i < sz && leaf_keys[i] < min) i++;

    // first index in the PMA
    i = values(leaf)[i];
    // find the first valid element
    element_t* __restrict elements = storage.m_elements;
    if(min < 0) min = 0;
    size_t size = storage.m_capacity;
    while(i < size && elements[i].key < min) i++;

    SumResult result;
    if(i >= size || elements[i].key > max) return result; // empty result
    result.m_first_key = elements[i].key;
    size_t last = i;

    while(i < size && elements[i].key <= max){
        if(elements[i].key >= 0){
            result.m_sum_keys += elements[i].key;
            result.m_sum_values += elements[i].value;
            result.m_num_elements++;
            last = i;
        } // end if
        i++;
    }

    result.m_last_key = elements[last].key;

    return result;
}


/*****************************************************************************
 *                                                                           *
 *   B-Tree nodes                                                            *
 *                                                                           *
 *****************************************************************************/

InternalNode* APMA_Baseline::create_internal_node() const {
    static_assert(!std::is_polymorphic<InternalNode>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(InternalNode) == 8, "Expected only 8 bytes for the cardinality");

    // (cardinality) 1 + (keys=) intnode_b + (pointers) intnode_b +1 == 2 * intnode_b +2;
    InternalNode* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */64,  /* size */ sizeof(InternalNode) + sizeof(int64_t) * (2 * index.intnode_b +1));
    if(rc != 0) throw std::runtime_error("APMA_Baseline::create_internal_node, cannot obtain a chunk of aligned memory");
    memset(ptr, 0, sizeof(InternalNode)); // => ptr->N = 0; ptr->is_leaf = false;
    return ptr;
}

Leaf* APMA_Baseline::create_leaf() const {
    static_assert(!std::is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + ptr previous + ptr next");

    // (cardinality) 1 + (ptr left/right) 2 + (keys=) leaf_b + (values) leaf_b +1 + (cardinalities) b +1 == 3 + (3*leaf_b +2);
    Leaf* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */64,  /* size */ sizeof(Leaf) +
            sizeof(decltype(keys(ptr)[0])) * index.leaf_b +
            sizeof(decltype(values(ptr)[0])) * (index.leaf_b +1) +
            sizeof(decltype(cardinalities(ptr)[0])) * (index.leaf_b + 1));
    if(rc != 0) throw std::runtime_error("APMA_Baseline::create_leaf, cannot obtain a chunk of aligned memory");
    memset(ptr, 1, 1); // ptr->is_leaf = true;
    ptr->N = 0;
    ptr->next = ptr->prev = nullptr;
    return ptr;
}

void APMA_Baseline::delete_node(Node* node) {
    if(node){
        if(node->is_leaf){
            delete_node(reinterpret_cast<Leaf*>(node));
        } else {
            delete_node(reinterpret_cast<InternalNode*>(node));
        }
    }
}

void APMA_Baseline::delete_node(InternalNode* inode){
    if(!inode) return;

    Node** c = children(inode);
    for(size_t i = 0; i < inode->N; i++){
        delete_node(c[i]);
    }

    free(inode);
}

void APMA_Baseline::delete_node(Leaf* leaf){
    if(!leaf) return;

    if(leaf->next){ leaf->next->prev = leaf->prev; }
    if(leaf->prev){ leaf->prev->next = leaf->next; }

    free(leaf);
}


int64_t* APMA_Baseline::keys(const InternalNode* inode) const{
    InternalNode* instance = const_cast<InternalNode*>(inode);
    return reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(instance) + sizeof(InternalNode));
}

Node** APMA_Baseline::children(const InternalNode* inode) const {
    return reinterpret_cast<Node**>(keys(inode) + index.intnode_b);
}

int64_t* APMA_Baseline::keys(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}

uint64_t* APMA_Baseline::values(const Leaf* leaf) const {
    return reinterpret_cast<uint64_t*>(keys(leaf) + index.leaf_b);
}

uint16_t* APMA_Baseline::cardinalities(const Leaf* leaf) const {
    return reinterpret_cast<uint16_t*>(values(leaf) + index.leaf_b +1);
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void APMA_Baseline::dump() const {
    dump(std::cout);
}

void APMA_Baseline::dump(std::ostream& out) const {
    out << "[Index] intnode_a: " << index.intnode_a << ", intnode_b: " << index.intnode_b <<
            ", leaf_a: " << index.leaf_a << ", leaf_b: " << index.leaf_b <<
            ", storage_a: " << index.storage_a << ", storage_b: " << index.storage_b << "\n";

    bool integrity_check = true;
    dump_node(out, index.root, 0, &integrity_check);

    out << "\n";
    out << "[Storage] PMA total capacity: " << storage.m_capacity << ", segment capacity: " << storage.m_segment_capacity <<
            ", number of segments: " << storage_get_num_segments() <<
            ", height: " << storage.m_height << ", cardinality: " << storage.m_cardinality << "\n";

    dump_storage(out);

    out << "\n";
    dump_predictor(out);

    out << std::endl;

    assert(integrity_check && "Integrity check failed!");
}

static void dump_tabs(std::ostream& out, size_t depth){
    using namespace std;

    auto flags = out.flags();
    out << setw(depth * 2 + 5) << setfill(' ') << ' ';
    out.setf(flags);
}

void APMA_Baseline::dump_node(std::ostream& out, Node* node, size_t depth, bool* integrity_check) const {
    using namespace std;

    assert(node != nullptr);

    // preamble
    auto flags = out.flags();
    if(depth > 0) out << ' ';
    out << setw(depth * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << (node->is_leaf ? "L" : "I") << ' ';
    out << hex << node << dec << " N: " << node->N << '\n' ;
    out.setf(flags);

    // Internal node
    if(!node->is_leaf) {
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        auto flags = out.flags();
        dump_tabs(out, depth);
        out << "Keys: ";
        if(inode->N > 0){
          for(size_t i = 0; i < inode->N -1; i++){
            if(i > 0) out << ", ";
            out << i << ": " << keys(inode)[i];
          }
        }
        out << '\n';
        dump_tabs(out, depth);
        out << "Ptrs: " << hex;
        for(size_t i = 0; i < inode->N; i++){
          if(i > 0) out << ", ";
          out << i << ": " << children(inode)[i];
        }
        out << dec << endl;
        out.setf(flags);

        // recursively dump the children
        for(size_t i = 0; i < inode->N; i++){
            dump_node(out, children(inode)[i], depth +1, integrity_check);
        }
    }

    // Leaf
    else {
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        auto flags = out.flags();

        dump_tabs(out, depth);
        out << "Keys: ";
        if(leaf->N > 0){
          for(size_t i = 0; i < leaf->N -1; i++){
            if(i > 0) out << ", ";
            out << i << ": " << keys(leaf)[i];
          }
        }

        out << '\n';
        dump_tabs(out, depth);
        out << "Values: ";
        for(size_t i = 0; i < leaf->N; i++){
            if(i > 0) out << ", ";
            out << i << ": " << values(leaf)[i] << " #" << cardinalities(leaf)[i];

            // validate that the value if pointing to an element in the storage
            if(storage.m_capacity <= values(leaf)[i] || storage.m_elements[values(leaf)[i]].key < 0){
                out << "(ERROR: no element in the storage)";
                if(integrity_check) *integrity_check = false;
            } else if (i > 0 && storage.m_elements[values(leaf)[i]].key != keys(leaf)[i-1]){
                out << "(ERROR: key mismatch in the storage)";
                if(integrity_check) *integrity_check = false;
            } else if ( cardinalities(leaf)[i] > index.storage_b ){
                out << "(ERROR: cardinality out of bound, > " << index.storage_b << ")";
                if(integrity_check) *integrity_check = false;
            } else if ( i > 0 && values(leaf)[i] <= values(leaf)[i-1] ){
                out << "(ERROR: order mismatch: " << values(leaf)[i-1] << " >= " << values(leaf)[i] << ")";
                if(integrity_check) *integrity_check = false;
            }
        }
        out << endl;
        out.setf(flags);
    }
}

void APMA_Baseline::dump_storage(ostream& out) const{
    for(size_t i = 0; i < storage_get_num_segments(); i++){
        out << "[" << i << "] ";
        size_t pos = i * storage.m_segment_capacity;
        bool first = true;
        while(!storage_is_cell_empty(pos)){
            if(!first) out << ", "; else first = false;
            out << "<[" << pos << "] " << storage.m_elements[pos].key << ", " << storage.m_elements[pos].value << ">";
            pos++;
        }
        out << "\n";
    }
}

void APMA_Baseline::dump_predictor(std::ostream& out) const {
    predictor.dump(out);
}

} // namespace pma

