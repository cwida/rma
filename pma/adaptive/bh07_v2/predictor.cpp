/*
 * predictor.cpp
 *
 *  Created on: Jul 10, 2018
 *      Author: dleo@cwi.nl
 */

#include "predictor.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>

#include "errorhandling.hpp"
#include "miscellaneous.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[Predictor::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


namespace pma { namespace adaptive { namespace bh07_v2 {


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

Predictor::Predictor(size_t size, size_t max_count) : m_buffer(nullptr){
    set_max_count(max_count);
    if(size <= 1){ throw std::invalid_argument("Invalid size: <= 1"); }

    m_capacity_max = hyperceil(size);
    m_buffer = allocate_buffer(m_capacity_max);
    m_capacity = size;
    m_tail = m_head = 0;
    m_empty = true;
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

/*****************************************************************************
 *                                                                           *
 *   Observers                                                               *
 *                                                                           *
 *****************************************************************************/

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

vector<PermutedItem> Predictor::weighted_elements(uint64_t min, uint64_t max){
    vector<PermutedItem> items;
    if(empty()) return items;

    items.reserve(m_capacity);

    auto buffer = m_buffer;
    auto append_if = [buffer, &items, min, max](size_t index){
        auto& item = buffer[index];
        assert(item.m_count > 0 && "Accessing an invalid element");

        if(min <= item.m_pointer && item.m_pointer <= max){
            items.push_back( PermutedItem{ item.m_pointer, item.m_count, index } );
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

    std::sort(begin(items), end(items), [](const PermutedItem& i1, const PermutedItem& i2){
       return i1.m_pointer < i2.m_pointer;
    });

    return items;
}

vector<PermutedItem> Predictor::items(uint64_t min, uint64_t max){
    return weighted_elements(min, max);
}

/*****************************************************************************
 *                                                                           *
 *   Update                                                                  *
 *                                                                           *
 *****************************************************************************/

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

void Predictor::reset_ptr(size_t index, size_t pma_position) {
    m_buffer[index].m_pointer = pma_position;
}

/*****************************************************************************
 *                                                                           *
 *   Resize                                                                  *
 *                                                                           *
 *****************************************************************************/

void Predictor::resize(size_t sz) {
    if(sz == m_capacity) return; // nop
    if(sz <= 1){ throw std::invalid_argument("Invalid size: <= 1"); }
    if(sz < size()) { throw std::invalid_argument("Cannot reduce the size of the data structure, it contains too many elements"); };

    if(empty()){
      m_capacity = sz;
    } else if(sz > m_capacity){
        if(sz <= m_capacity_max){
            if(m_tail < m_head){
                m_capacity = sz;
            } else if (m_head == 0){ // as above
                m_head = m_capacity;
                m_capacity = sz;
            } else { // m_head <= m_tail
                size_t leftover = m_head;

                size_t end = min(sz, m_capacity + m_head);
                for(size_t i  = m_capacity; i < end; i++){
                    size_t j = i % m_capacity;
                    m_buffer[i] = m_buffer[j];
                }
                size_t copied = end - m_capacity;
                leftover -= copied;

                if(leftover == 0){
                    memset(m_buffer, 0, m_head * sizeof(m_buffer[0]));
                    m_head = end % sz;
                } else {
                    for(size_t i = 0; i < leftover; i++){
                        m_buffer[i] = m_buffer[copied +i];
                    }
                    memset(m_buffer + leftover, 0, (m_head - leftover) * sizeof(m_buffer[0]));
                    m_head = leftover;
                }

                m_capacity = sz;
            }
        } else { //sz > m_capacity_max
            resize_with_new_buffer(sz);
        }
    } else { // sz < m_capacity
        resize_with_new_buffer(sz);
    }
}

void Predictor::resize_with_new_buffer(size_t sz){
    assert( sz >= size() );

    size_t capacity_max1 = hyperceil(sz);
    size_t capacity1 = sz;

    Item* __restrict buffer1 = allocate_buffer(capacity_max1);

    size_t i = 0;
    if( m_head > m_tail ) {
        for(size_t j = m_tail; j < m_head; j++){
            buffer1[i] = m_buffer[j];
            i++;
        }
    } else { // m_head <= m_tail
        for(size_t j  = m_tail; j < m_capacity; j++){
            buffer1[i] = m_buffer[j];
            i++;
        }
        for(size_t j = 0; j < m_head; j++){
            buffer1[i] = m_buffer[j];
            i++;
        }
    }
    m_tail = 0;
    m_head = i;
    m_capacity = capacity1;
    m_capacity_max = capacity_max1;

    deallocate_buffer(m_buffer);
    m_buffer = buffer1;
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

void Predictor::dump(std::ostream& out) const {
    out << "[Predictor] current capacity: " << m_capacity << ", max capacity: " << m_capacity_max << ", tail: " << m_tail << ", head: " << m_head << ", max count: " << m_count_max << ", size: " << size() << endl;
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


std::ostream& operator<<(std::ostream& out, const PermutedItem& item) {
    out << "pointer: " << item.m_pointer << ", count: " << item.m_count << ", permuted position: " << item.m_permuted_position;
    return out;
}


}}} // pma::adaptive::bh07_v2
