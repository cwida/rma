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

#include "pma_v4.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring> // memcpy
#include <iostream>
#include <memory>
#include <limits>
#include <stdexcept>
#include <vector>

namespace pma {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[PMA_Impl4::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Helpers                                                                 *
 *                                                                           *
 *****************************************************************************/
// 2^ceil(log2(x))
static size_t hyperceil(std::size_t value){
    return (std::size_t) pow(2, ceil(log2(value)));
}

size_t PMA_Impl4::size() const {
    return m_cardinality;
}

bool PMA_Impl4::empty() const {
    return m_cardinality == 0;
}

size_t PMA_Impl4::get_num_segments() const{
    return m_capacity / m_segment_capacity;
}


/*****************************************************************************
 *                                                                           *
 *   Initialization                                                          *
 *                                                                           *
 *****************************************************************************/
PMA_Impl4::PMA_Impl4() : m_elements(nullptr), m_workspace(nullptr) {
    initialize(min_capacity);
}

PMA_Impl4::~PMA_Impl4() {
    delete[] m_elements; m_elements = nullptr;
    delete[] m_workspace; m_workspace = nullptr;
    delete[] m_segments; m_segments = nullptr;
}

void PMA_Impl4::initialize(size_t capacity) {
    m_capacity = hyperceil(capacity);
    m_segment_capacity = m_capacity;
    m_height = 1;
    if(m_elements != nullptr) { delete[] m_elements; m_elements = nullptr; }
    try {
        m_elements = new element_t[m_capacity];
    } catch( const std::bad_alloc& e){
        std::cerr << "[PMA_Impl4::initialize] Cannot allocate a new array of capacity: " << this->m_capacity << std::endl;
        throw;
    }
    m_cardinality = 0;

    if(capacity >= std::numeric_limits<uint16_t>::max()){
        throw std::runtime_error("Invalid minimum capacity");
    }
    m_segments = new uint16_t[1]();

    m_workspace = new element_t[workspace_max_size];
}



void PMA_Impl4::clear(){
    m_cardinality = 0;
    initialize(min_capacity);
}

/*****************************************************************************
 *                                                                           *
 *   Rebalance                                                               *
 *                                                                           *
 *****************************************************************************/
void PMA_Impl4::window_bounds(size_t position, size_t height, int64_t& out_lb, int64_t& out_ub) const {
  assert(height > 0 && height <= this->m_height);
  size_t capacity = m_segment_capacity * pow(2, height -1);
  double gid = ((double) position) / capacity;
  int64_t lb = (size_t) (floor(gid) * capacity);

  out_lb = lb;
  out_ub = lb + capacity -1;
}

void PMA_Impl4::window_thresholds(std::size_t height, double& out_a, double& out_b) const {
  assert(height > 0 && height <= this->m_height);
  double diff = (((double) this->m_height) - height) / this->m_height;
  out_a = r_0 - 0.25 * diff;
  out_b = t_0 + 0.25 * diff;
}

void PMA_Impl4::rebalance(size_t segment_id) {
    int height = 1;
    int window_length = 1;
    int window_id = segment_id;
    int window_start = segment_id, window_end;
    double density = 1.0, rho, theta = t_0;
    size_t num_elements = m_segments[segment_id];

    if(m_height > 1){
        // find the bounds of this window
        int index_left = segment_id -1;
        int index_right = segment_id +1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;
            window_thresholds(height, rho, theta);

            // find the number of elements in the interval
            while(index_left >= window_start){
                num_elements += m_segments[index_left];
                index_left--;
            }
            while(index_right < window_end){
                num_elements += m_segments[index_right];
                index_right++;
            }

            COUT_DEBUG("num_elements: " << num_elements << ", window_start: " << window_start << ",  window_length: " << window_length << ",  segment_capacity: " << m_segment_capacity);
            density = ((double) num_elements) / (window_length * m_segment_capacity);

            COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta);
        } while ((density >= theta) && height < m_height);
    }

    if(density >= theta){
        resize();
    } else {
        spread(num_elements, window_start, window_length);
    }
}

void PMA_Impl4::resize(){
    // compute the new capacity
    size_t capacity = m_capacity *2;
    COUT_DEBUG("new capacity: " << capacity);
    size_t segment_size = hyperceil(log2(capacity));
    size_t num_segments = capacity / segment_size;
    std::unique_ptr<element_t[]> elements_ptr{ new element_t[capacity] };
    auto* __restrict elements = elements_ptr.get();

    // populate the vector for the segment sizes
    size_t elements_per_segments = m_cardinality / num_segments;
    size_t odd_segments = m_cardinality % num_segments;
    std::unique_ptr<uint16_t[]> segments_ptr{ new uint16_t[num_segments] };
    uint16_t* __restrict segments = segments_ptr.get();
    for(size_t i = 0; i < num_segments; i++){
        segments[i] = elements_per_segments + (i < odd_segments);
    }

    auto it_ptr = iterator();
    auto it = it_ptr.get();

    // copy the elements
    for(size_t i = 0; i < num_segments; i++){
        auto* __restrict current_segment = elements + (segment_size * i);
        for(size_t j = 0; j < segments[i]; j++){
            current_segment[j] = it->next();
        }
    }

    // clean up
    delete[] m_elements; m_elements = nullptr;
    delete[] m_segments; m_segments = nullptr;
    m_capacity = capacity;
    m_segment_capacity = segment_size;
    m_elements = elements; elements_ptr.release();
    m_segments = segments; segments_ptr.release();
    m_height = log2(capacity / segment_size) +1;
}

void PMA_Impl4::spread(size_t num_elements, size_t window_start, size_t window_length){
    std::unique_ptr<element_t[]> tmp_ptr; // delete[] tmp* when it goes out of scope
    element_t* tmp (nullptr);
    if(num_elements > workspace_max_size){
        tmp_ptr.reset(new element_t[num_elements]);
        tmp = tmp_ptr.get();
    } else {
        tmp = m_workspace;
    }

    // copy the elements in the workspace
    size_t pos = 0;
    const size_t window_end = window_start + window_length; // exclusive;
    for(size_t i = window_start; i < window_end; i++){
        COUT_DEBUG("window: " << i << ", pos: " << pos << ", cardinality: " << m_segments[i]);
        memcpy(tmp + pos, m_elements + i * m_segment_capacity, sizeof(m_elements[0]) * m_segments[i]);
        pos += m_segments[i];
    }

    // adjust the size of the involved segments
    size_t elements_per_segment = num_elements / window_length;
    size_t odd_segments = num_elements % window_length;
    for(size_t i = 0; i < window_length; i++){
        m_segments[window_start + i] = elements_per_segment + (i < odd_segments);
    }

    // copy the elements to the final segments
    pos = 0;
    for(size_t i = window_start; i < window_end; i++){
        memcpy(m_elements + i * m_segment_capacity, tmp + pos, m_segments[i] * sizeof(m_elements[0]));
        pos += m_segments[i];
    }
}

/*****************************************************************************
 *                                                                           *
 *   Insertion                                                               *
 *                                                                           *
 *****************************************************************************/
void PMA_Impl4::insert(/*const*/ key_t/*&*/ key, /*const*/ value_t/*&*/ value){
    COUT_DEBUG("key: " << key << ", value: " << value);
    auto segment_id = find_segment(key);
    COUT_DEBUG("target segment: " << segment_id);

    assert(m_segments[segment_id] < m_segment_capacity && "The segment should not be full");
    int64_t size = m_segments[segment_id];
    auto* __restrict elements = m_elements + (m_segment_capacity * segment_id);
    size_t i = size;
    while( i > 0 && key < elements[i -1].first) {
        elements[i] = elements[i-1];
        i--;
    }
    elements[i].first = key;
    elements[i].second = value;

    m_segments[segment_id]++;
    m_cardinality++;

    if(m_segments[segment_id] == m_segment_capacity){
        rebalance(segment_id);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/
size_t PMA_Impl4::find_segment(key_t key) const noexcept {
    if(empty()) return 0;

    size_t lb = 0; // inclusive
    size_t ub = get_num_segments() -1; // inclusive
    COUT_DEBUG("key: " << key << ", lb: " << lb << ", ub: " << ub);

    while(lb < ub){
        size_t pivot = (lb + ub) / 2;
        COUT_DEBUG("lb: " << lb << ", ub: " << ub << ", pivot: " << pivot);

        // move left
        assert(m_segments[pivot] > 0 && "No segments should be empty");
        if(key < m_elements[pivot * m_segment_capacity].first){
            ub = pivot -1;
        }

        else if (key <= m_elements[pivot * m_segment_capacity + m_segments[pivot] -1].first){ // found
            return pivot;

        } else { // move right;
            lb = pivot +1;
        }
    }

    return lb;
}

PMA_Impl4::value_t PMA_Impl4::find(key_t key) const noexcept {
    auto segment_id = find_segment(key);

    auto* __restrict elements = m_elements + m_segment_capacity * segment_id;
    for(size_t i = 0, sz = m_segments[segment_id]; i < sz; i++){
        if(elements[i].first == key){
            return elements[i].second;
        }
    }

    // not found
    return -1;
}

/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/

PMA_Impl4::Iterator::Iterator(const PMA_Impl4* instance) : m_instance(instance) {
    m_segment = 0;
    m_current = 0;
    m_stop = m_instance->m_segments[m_segment];
    m_end = m_instance->m_capacity;
}

void PMA_Impl4::Iterator::move(){
    m_current++;
    if(m_current >= m_stop){
        m_segment++;
        m_current = m_instance->m_segment_capacity * m_segment;
        if(m_current < m_end){
            if(m_segment < m_instance->get_num_segments()){
                m_stop = m_current + m_instance->m_segments[m_segment];
            }
            if(m_stop > m_end)
                m_stop = m_end;
        }
    }
}

bool PMA_Impl4::Iterator::hasNext() const {
    return m_current < m_stop;
}

std::pair<PMA_Impl4::key_t, PMA_Impl4::value_t> PMA_Impl4::Iterator::next(){
    assert(hasNext());
    auto res = m_instance->m_elements[m_current];
    move();
    return res;
}

std::unique_ptr<pma::Iterator> PMA_Impl4::iterator() const{
    std::unique_ptr<pma::Iterator> ptr(new PMA_Impl4::Iterator(this));
    return ptr;
}

/*****************************************************************************
 *                                                                           *
 *   Sum                                                                     *
 *                                                                           *
 *****************************************************************************/

pma::Interface::SumResult PMA_Impl4::sum(int64_t min, int64_t max) const {
    if(empty() || min > max) return SumResult{};
    auto segment_start = find_segment(min);
    auto segment_end = find_segment(max);

    // scan the first segment
    auto* __restrict elements = m_elements + m_segment_capacity * segment_start;
    size_t i = 0, sz = m_segments[segment_start];
    while(i < sz && elements[i].first < min) i++;
    if(i == sz || elements[i].first > max) return SumResult{};
    SumResult result;
    result.m_first_key = elements[i].first;
    while(i < sz && elements[i].first <= max){
        result.m_sum_keys += elements[i].first;
        result.m_sum_values += elements[i].second;
        result.m_last_key = elements[i].first;
        result.m_num_elements++;
        i++;
    }
    if(i < sz || segment_start == segment_end) return result;

    for(size_t segment_id = segment_start +1; segment_id < segment_end; segment_id++){
        elements = m_elements + m_segment_capacity * segment_id;
        for(size_t i = 0, sz = m_segments[segment_id]; i < sz; i++){
            result.m_sum_keys += elements[i].first;
            result.m_sum_values += elements[i].second;
            result.m_num_elements++;
        }
    }

    elements = m_elements + m_segment_capacity * segment_end;
    i = 0; sz = m_segments[segment_end];
    while(i < sz && elements[i].first <= max){
        result.m_sum_keys += elements[i].first;
        result.m_sum_values += elements[i].second;
        result.m_last_key = elements[i].first;
        result.m_num_elements++;
        i++;
    }

    return result;
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void PMA_Impl4::dump() const{
    using namespace std;
    cout << "PMA, capacity: " << m_capacity << ", segment size: " << m_segment_capacity << ", height: " << m_height << ", cardinality: " << m_cardinality << endl;
    for(size_t i = 0; i < get_num_segments(); i++){
        cout << "[" << i << "] ";
        auto* elements = m_elements + i * m_segment_capacity;
        for(size_t j = 0; j < m_segments[i]; j++){
            if(j > 0) cout << ", ";
            cout << "<" << elements[j].first << ", " << elements[j].second << ">";
        }
        cout << "\n";
    }
    cout << endl;
}

} // namespace pma
