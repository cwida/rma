/*
 * detector.cpp
 *
 *  Created on: Mar 6, 2018
 *      Author: dleo@cwi.nl
 */

#include "detector.hpp"

#include <cassert>
#include <cstring>
#include <iostream>
#include "knobs.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

Detector::Detector(Knobs& knobs, size_t size, uint32_t modulus) : m_knobs(knobs), m_capacity(size), m_sizeof_entry(modulus + 3), m_modulus(modulus) {
    m_buffer = new int64_t[ m_capacity * m_sizeof_entry ]();
}

Detector::~Detector() {
   delete[] m_buffer; m_buffer = nullptr;
}

void Detector::clear(){
    memset(m_buffer, 0, (m_sizeof_entry * m_capacity) * sizeof(m_buffer[0]));
}

void Detector::clear(uint64_t segment_id){
    assert(segment_id < m_capacity);
    int64_t* __restrict section = m_buffer + segment_id * m_sizeof_entry;
    for(int64_t i =0, sz = m_sizeof_entry; i < sz; i++){
        section[i] = 0;
    }
}

void Detector::resize(size_t size){
    delete[] m_buffer; m_buffer = nullptr;

    m_capacity = size;
    m_buffer = new int64_t[m_sizeof_entry * size]();
}

void Detector::dump(std::ostream& out) const {
    out << "[Detector] size: " << m_capacity << ", recordings per segment: " << (int) m_modulus << "\n";

    for(size_t i = 0; i < m_capacity; i++){
        int64_t* section = m_buffer + m_sizeof_entry * i;

        // header
        int16_t head = reinterpret_cast<int16_t*>(section)[0];
        int16_t count_forward = reinterpret_cast<int16_t*>(section)[1];
        int16_t count_backwards = reinterpret_cast<int16_t*>(section)[2];
        int16_t count_segment = reinterpret_cast<int16_t*>(section)[3];
        section++;
        int64_t key_forward = section[0];
        int64_t key_backward = section[1];
        out << "[" << i << "] sc: " << count_segment << ", fwd: " << key_forward << "(" << count_forward << "), bwd: " << key_backward << "(" << count_backwards << "); ts [head=" << head << "]: ";

        section += 2;

        // timestamps
        bool first = true;
        for(int h = head -1; h >= 0; h--){
            if(section[h] != 0){
                if(first) first = false; else out << ", ";
                out << section[h];
            }
        }
        for(int h = m_modulus -1; h >= head; h--){
            if(section[h] != 0){
                if(first) first = false; else out << ", ";
                out << section[h];
            }
        }
        out << '\n';
    }

    flush(out);
}

void Detector::dump() const {
    dump(::std::cout);
}

template<int increment>
void Detector::update(size_t segment_id, int64_t predecessor, int64_t successor){
    update<increment>(segment_id, predecessor, successor, ++m_counter);
}

template<int increment>
void Detector::update(size_t segment, int64_t predecessor, int64_t successor, int64_t timestamp){
    assert(segment < m_capacity);

    int16_t sequence_count_max = static_cast<int16_t>(m_knobs.get_max_sequence_counter());
    int16_t sequence_count_min = -sequence_count_max;
    int16_t segment_count_max = static_cast<int16_t>(m_knobs.get_max_segment_counter());

    int64_t* __restrict section = m_buffer + m_sizeof_entry * segment;
    int16_t& head = reinterpret_cast<int16_t*>(section)[0];
    int16_t& count_fwd = reinterpret_cast<int16_t*>(section)[1];
    int16_t& count_bwd = reinterpret_cast<int16_t*>(section)[2];
    int16_t& count_segment = reinterpret_cast<int16_t*>(section)[3];
    int64_t& key_fwd = section[1];
    int64_t& key_bwd = section[2];

    // segment counter
    if(count_segment * increment >= 0){
        if((count_segment * increment) < segment_count_max){
            count_segment += increment;
        }
    } else { // decrease by 2
        count_segment = count_segment + 2 * increment;
    }

    // sequence counter
    if(successor == key_bwd){ // direction forwards
        if((increment > 0 && count_bwd < sequence_count_max) || (increment < 0 && count_bwd > sequence_count_min)){
            count_bwd += increment;
        }
    } else if (predecessor == key_fwd){ // direction backwards
        if((increment > 0 && count_fwd < sequence_count_max) || (increment < 0 && count_fwd > sequence_count_min)){
            count_fwd += increment;
        }
    } else {
        if(count_fwd == 0){
            count_fwd += increment;
            key_fwd = predecessor;
        } else {
            count_fwd -= increment;
        }
        if(count_bwd == 0){
            count_bwd += increment;
            key_bwd = successor;

        } else {
            count_bwd -= increment;
        }
    }

    // timestamp
    section[3 + head] = timestamp;
    head = ( head +1 ) % m_modulus;
}

void Detector::insert(uint64_t segment, int64_t predecessor, int64_t successor){
    update<+1>(segment, predecessor, successor);
}

void Detector::remove(uint64_t segment, int64_t predecessor, int64_t successor){
    update<-1>(segment, predecessor, successor);
}

size_t Detector::size() const {
    return m_capacity;
}
size_t Detector::capacity() const{
    return size();
}
int64_t* Detector::buffer() const{
    return m_buffer;
}
size_t Detector::sizeof_entry() const{
    return m_sizeof_entry;
}
size_t Detector::modulus() const{
    return m_modulus;
}

}}} // pma::adaptive::int1
