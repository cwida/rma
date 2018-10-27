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
#include "cbytearray.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace std;

namespace distribution {

size_t CByteArray::compute_bytes_per_elements(size_t value){
    double bits = ceil(log2(value));
    double bytes = ceil(bits / 8);
    return size_t(bytes);
}

CByteArray::CByteArray(size_t capacity) : CByteArray(compute_bytes_per_elements(capacity), capacity) { }

CByteArray::CByteArray(size_t bytes_per_element, size_t capacity) : m_bytes_per_element(bytes_per_element), m_capacity(capacity), m_array(nullptr){
//    cout << "bytes_per_element: " << bytes_per_element << endl;
    if(bytes_per_element <= 0 || bytes_per_element > 8)
        throw std::invalid_argument(std::string("Invalid value for the parameter bytes_per_elements: ") + std::to_string(bytes_per_element));
    m_array = new char[m_bytes_per_element * m_capacity];
}

CByteArray::CByteArray(CByteArray&& tmp): m_bytes_per_element(tmp.m_bytes_per_element), m_capacity(tmp.m_capacity), m_array(tmp.m_array){
    tmp.m_array = nullptr;
}

CByteArray::~CByteArray() {
    delete[] m_array;
}

CByteArray& CByteArray::operator=(CByteArray&& tmp){
    delete[] m_array;

    m_bytes_per_element = tmp.m_bytes_per_element;
    m_capacity = tmp.m_capacity;
    m_array = tmp.m_array;

    tmp.m_array = nullptr;
    return *this;
}


int64_t CByteArray::get_value_at(size_t index) const {
    union {
      int64_t key;
      char bytes[8];
    } u;
    static_assert(sizeof(u) == 8, "Expected 8 bytes long");
    u.key = 0;
    size_t base = index * m_bytes_per_element;
    for(size_t i =0; i < m_bytes_per_element; i++){
        // intel is little endian
//        u.bytes[8 - bytes_per_element + i] = array[base + i];
        u.bytes[i] = m_array[base + i];
    }
    return u.key;
}

#include <vector>

void CByteArray::set_value_at(size_t index, int64_t value) {
    union {
       int64_t value;
       char bytes[8];
    } u;
    static_assert(sizeof(u) == 8, "Expected 8 bytes long");
    u.value = value;
    size_t base = index * m_bytes_per_element;
    for(size_t i = 0; i < m_bytes_per_element; i++){
        // intel is little endian
//        array[base + i] = u.bytes[8 - bytes_per_element + i];
        m_array[base + i] = u.bytes[i];
    }
}

CByteReference CByteArray::operator[](size_t index){
    return CByteReference(this, index);
}

const CByteReference CByteArray::operator[](size_t index) const{
    return CByteReference(const_cast<CByteArray*>(this), index);
}

size_t CByteArray::capacity() const {
    return m_capacity;
}

CByteIterator CByteArray::begin(){
    return CByteIterator{this, 0};
}

CByteIterator CByteArray::end(){
    return CByteIterator{this, m_capacity};
}

void CByteArray::sort(){
    std::sort(begin(), end());
}

void CByteArray::sort(size_t start, size_t end){ // sort in [start, end)
    assert(start < m_capacity && "Index out of bound");
    std::sort(CByteIterator{this, start}, CByteIterator{this, min(end, m_capacity)});
}


unique_ptr<CByteArray> CByteArray::merge(CByteArray** arrays, size_t arrays_sz){
    size_t capacity = 0;
    size_t bytes_per_element = 0;
    for(size_t i = 0; i < arrays_sz; i++){
        CByteArray* ba = arrays[i];
        if(ba == nullptr) continue;
        capacity += ba->capacity();
        if(!(bytes_per_element == 0 || bytes_per_element == ba->m_bytes_per_element)){
            throw std::runtime_error("[CByteArray::merge] Different values for bytes_per_element");
        }
        bytes_per_element = ba->m_bytes_per_element;
    }

    if(bytes_per_element == 0){
        assert(capacity == 0); // otherwise when updating the capacity this value should have changed
        bytes_per_element = 1;
    }


    CByteArray* result = new CByteArray(bytes_per_element, capacity);
    size_t destination = 0;
    for(size_t i = 0; i < arrays_sz; i++){
        CByteArray* ba = arrays[i];
        if(ba == nullptr) continue;
        size_t amount = bytes_per_element * ba->m_capacity;
        memcpy(result->m_array + destination, ba->m_array, amount);
        destination += amount;
    }

    return unique_ptr<CByteArray>{result};
}

/*****************************************************************************
 *                                                                           *
 *   Reference                                                               *
 *                                                                           *
 *****************************************************************************/
CByteReference::CByteReference(CByteArray* array, size_t index) : m_array(array), m_index(index) { }

CByteReference::operator int64_t() const {
    return m_array->get_value_at(m_index);
}

CByteReference& CByteReference::operator =(int64_t value){
    m_array->set_value_at(m_index, value);
    return *this;
}

CByteReference& CByteReference::operator =(const CByteReference& value) {
    m_array->set_value_at(m_index, value);
    return *this;
}

void swap(CByteReference x, CByteReference y) {
    int64_t tmp = x;
    x = y;
    y = tmp;
}

/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/
CByteIterator::CByteIterator() : m_array(nullptr), m_offset(0) { }

CByteIterator::CByteIterator(CByteArray* array, size_t position) : m_array(array), m_offset(position) { }

bool CByteIterator::operator==(const CByteIterator& e) const {
    return m_array == e.m_array && m_offset == e.m_offset;
}

bool CByteIterator::operator!=(const CByteIterator& e) const{
    return !(*this == e);
}

bool CByteIterator::operator<(const CByteIterator& e) const {
    return m_offset < e.m_offset;
}

bool CByteIterator::operator>(const CByteIterator& e) const {
    return e < *this;
}

bool CByteIterator::operator<=(const CByteIterator& e) const {
    return !(e < *this);
}

bool CByteIterator::operator>=(const CByteIterator& e) const {
    return !(*this < e);
}

CByteReference CByteIterator::operator*() const {
    return CByteReference(m_array, m_offset);
}

CByteReference CByteIterator::operator[](std::ptrdiff_t d) const {
    return CByteReference(m_array, static_cast<int64_t>(m_offset) + d);
}

CByteIterator& CByteIterator::operator++() { // x++
    m_offset++;
    return *this;
}

CByteIterator CByteIterator::operator++(int) { // ++x
    return CByteIterator(m_array, m_offset++);
}

CByteIterator& CByteIterator::operator+=(ptrdiff_t d) { // x += d
    m_offset += d;
    return *this;
}

CByteIterator CByteIterator::operator+(std::ptrdiff_t d){ // x + d
    return CByteIterator(m_array, m_offset + d);
}

CByteIterator& CByteIterator::operator--() { // x--
    m_offset--;
    return *this;
}

CByteIterator CByteIterator::operator--(int) { // --x
    return CByteIterator(m_array, m_offset--);
}

CByteIterator& CByteIterator::operator-=(ptrdiff_t d) { // x -= d
    m_offset -= d;
    return *this;
}

CByteIterator CByteIterator::operator-(std::ptrdiff_t d){ // x - d
    return CByteIterator(m_array, m_offset - d);
}

ptrdiff_t CByteIterator::operator+(const CByteIterator& it) const { // d = x + y
    return m_offset + it.m_offset;
}

ptrdiff_t CByteIterator::operator-(const CByteIterator& it) const { // d = x - y
    return m_offset - it.m_offset;
}



} // namespace distribution
