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

#include "external.hpp"

#include <cassert>
#include <iostream>
#include <utility> // std::pair

#include "errorhandling.hpp"

/*****************************************************************************
 *                                                                           *
 *   Gaurav Menghani / impl1.hpp                                             *
 *                                                                           *
 *****************************************************************************/
#include "external/menghani/impl1.hpp"

namespace pma {

namespace detail_menghani_impl1 {

struct Element : public std::pair<int64_t, int64_t> {
    Element() { }
    Element(int64_t k, int64_t v) : std::pair<int64_t, int64_t>(k, v) { }
    bool operator == (const Element& e){ return first == e.first; }
    bool operator != (const Element& e){ return ! operator ==(e); }
    bool operator < (const Element& e){ return first < e.first; }
    bool operator > (const Element& e){ return ! operator <(e); }
};

std::ostream& operator<< (std::ostream& o, const Element& e){
    o << "<" << e.first << ", " << e.second << ">";
    return o;
}

using PMA_t = reddragon::impl1::PackedMemoryArray<Element>;

class IteratorImpl : public pma::Iterator {
    friend class ::pma::PMA_Menghani_1;
    PMA_t& pma;
    int pos;

    IteratorImpl(PMA_t& pma);
    void move();

public:
    bool hasNext() const;
    std::pair<int64_t, int64_t> next();
};

IteratorImpl::IteratorImpl(PMA_t& pma): pma(pma), pos(0) {
    move();
}

void IteratorImpl::move() {
    while(pos < pma.size() && !pma.elem_exists_at(pos)) pos++;
}


bool IteratorImpl::hasNext() const{
    return pos < pma.size();
}

std::pair<int64_t, int64_t> IteratorImpl::next(){
    assert(hasNext());
    auto p = pma.elem_at(pos);
    pos++; move();
    return std::make_pair(p.first, p.second);
}


} // namespace detail_menghani_impl1



#define IMPL reinterpret_cast<detail_menghani_impl1::PMA_t*>(impl)

PMA_Menghani_1::PMA_Menghani_1() : impl(nullptr){
    impl = new detail_menghani_impl1::PMA_t{ detail_menghani_impl1::Element{ 0, 0 } };
}

PMA_Menghani_1::~PMA_Menghani_1() {
    delete IMPL; impl = nullptr;
}

void PMA_Menghani_1::insert(int64_t key, int64_t value){
    detail_menghani_impl1::Element e{key, value};
    IMPL->insert_element(e);
}

int64_t PMA_Menghani_1::find(int64_t key) const {
    detail_menghani_impl1::Element dummy{key, -1};
    auto index = IMPL->find(dummy);
    if(index >= 0){
        return IMPL->elem_at(index).second;
    } else {
        return -1;
    }
}

std::size_t PMA_Menghani_1::size() const {
    return IMPL->size();
}

void PMA_Menghani_1::dump() const {
    IMPL->print();
}

std::unique_ptr<pma::Iterator> PMA_Menghani_1::iterator() const {
    std::unique_ptr<pma::Iterator> ptr(new detail_menghani_impl1::IteratorImpl(*(IMPL)));
    return ptr;
}

pma::Interface::SumResult PMA_Menghani_1::sum(int64_t min, int64_t max) const {
    RAISE_EXCEPTION(Exception, "Method ::sum(int64_t min, int64_t max) not implemented!");
}

#undef IMPL

} // namespace pma
/*****************************************************************************
 *                                                                           *
 *   Gaurav Menghani / impl2.hpp                                             *
 *                                                                           *
 *****************************************************************************/
#include "external/menghani/impl2.hpp"

namespace pma {

namespace detail_menghani_impl2 {

struct Element : public std::pair<int64_t, int64_t> {
    Element() { }
    Element(int64_t k, int64_t v) : std::pair<int64_t, int64_t>(k, v) { }
    bool operator == (const Element& e){ return first == e.first; }
    bool operator != (const Element& e){ return ! operator ==(e); }
    bool operator < (const Element& e){ return first < e.first; }
    bool operator > (const Element& e){ return ! operator <(e); }
};

std::ostream& operator<< (std::ostream& o, const Element& e){
    o << "<" << e.first << ", " << e.second << ">";
    return o;
}

using PMA_t = reddragon::impl2::PMA<Element>;
using PMAIterator_t = PMA_t::PMAIterator;

class IteratorImpl2 : public pma::Iterator {
    friend class ::pma::PMA_Menghani_2;
    PMAIterator_t pos;
    PMAIterator_t end;

    IteratorImpl2(const PMAIterator_t& begin, const PMAIterator_t& end);

public:
    bool hasNext() const;
    std::pair<int64_t, int64_t> next();
};

IteratorImpl2::IteratorImpl2(const PMAIterator_t& begin, const PMAIterator_t& end): pos(begin), end(end) {
    if(pos!=end && !pos.pma->present[pos.i]){
        ++pos;
    }
}

bool IteratorImpl2::hasNext() const{
    return pos != end;
}

std::pair<int64_t, int64_t> IteratorImpl2::next(){
    assert(hasNext());
    auto p = *pos;
    ++pos;
    return std::make_pair(p.first, p.second);
}

} // namespace detail_menghani_impl2


#define IMPL reinterpret_cast<detail_menghani_impl2::PMA_t*>(implementation)

PMA_Menghani_2::PMA_Menghani_2(std::size_t capacity) : implementation(new detail_menghani_impl2::PMA_t(capacity)){

}

PMA_Menghani_2::~PMA_Menghani_2() {
    delete IMPL; implementation = nullptr;
}

void PMA_Menghani_2::insert(int64_t key, int64_t value){
    IMPL->insert(detail_menghani_impl2::Element{key, value});
}

int64_t PMA_Menghani_2::find(int64_t key) const {
    detail_menghani_impl2::Element searchElement{key, -1};
    detail_menghani_impl2::PMA_t& impl = *(IMPL);
    auto lb = impl.lower_bound(detail_menghani_impl2::Element{key, -1});
    if(lb >= impl.impl.size()){ return -1; }
    auto ub = lb + impl.chunk_size;
    while(lb < ub && ( (!impl.present[lb]) || impl.impl[lb] < searchElement)){ lb++; }
    if(lb < ub && impl.present[lb] && impl.impl[lb].first == key){
        return impl.impl[lb].second;
    } else {
        return -1;
    }
}

std::size_t PMA_Menghani_2::size() const {
    return IMPL->size();
}

void PMA_Menghani_2::dump() const {
    IMPL->print();
}

std::unique_ptr<pma::Iterator> PMA_Menghani_2::iterator() const {
    std::unique_ptr<pma::Iterator> ptr(new detail_menghani_impl2::IteratorImpl2(std::begin(*(IMPL)), std::end(*(IMPL))));
    return ptr;
}

pma::Interface::SumResult PMA_Menghani_2::sum(int64_t min, int64_t max) const {
    RAISE_EXCEPTION(Exception, "Method ::sum(int64_t min, int64_t max) not implemented!");
}

#undef IMPL

} // namespace pma

/*****************************************************************************
 *                                                                           *
 *   Pablo Montes                                                            *
 *                                                                           *
 *****************************************************************************/
#include "external/montes/pma.h"

namespace pma {

#define _PMA (reinterpret_cast<PMA>(this->m_implementation))

PMA_Montes::PMA_Montes() : m_implementation(pma_create()) {

}

PMA_Montes::~PMA_Montes() {
    auto ptr = _PMA;
    pma_destroy(&ptr);
    m_implementation = nullptr;
}

void PMA_Montes::insert(int64_t key, int64_t value){
    pma_insert(_PMA, key, value);
}

int64_t PMA_Montes::find(int64_t key) const {
    int64_t index = -1;
    bool result = pma_find(_PMA, key, &index);
    if(result){
        pma_element_t element;
        pma_get(_PMA, index, &element);
        return element.val;
    } else {
        return -1;
    }
}

std::size_t PMA_Montes::size() const {
    return pma_count(_PMA);
}

std::size_t PMA_Montes::capacity() const {
    return pma_capacity(_PMA);
}

namespace detail_pabmont {

class IteratorImpl : public pma::Iterator {
    friend class ::pma::PMA_Montes;
    void* m_implementation; // pointer to the implementation
    int64_t m_position; // current position in the iterator
    const int64_t m_end; // last position (+1) for the iterator

    IteratorImpl(void* impl, int64_t begin, int64_t end);
    void move();

public :
    ~IteratorImpl();

    bool hasNext() const;
    std::pair<int64_t, int64_t> next();
};

IteratorImpl::IteratorImpl(void* impl, int64_t begin, int64_t end): m_implementation(impl), m_position(begin), m_end(end){
    assert(impl != nullptr);
    assert(begin >= 0 && m_end <= pma_capacity(_PMA));
    move();
}

IteratorImpl::~IteratorImpl(){ }

void IteratorImpl::move() {
    while(m_position < m_end){
        pma_element_t element;
        pma_get(_PMA, m_position, &element);
        if(keyval_empty(&element)){
            m_position++;
        } else { // found
            break;
        }
    }
}

bool IteratorImpl::hasNext() const {
    return m_position < m_end;
}

std::pair<int64_t, int64_t> IteratorImpl::next(){
    assert(m_position < m_end && "Index out of bounds");

    pma_element_t element;
    pma_get(_PMA, m_position, &element);
    assert(!keyval_empty(&element) && "Empty slot");
    auto result = std::make_pair(element.key, element.val);

    m_position++; move(); // move to the next item

    return result;
}

} // namespace detail_pabmont


std::pair<int64_t, int64_t> PMA_Montes::find_interval(int64_t min, int64_t max) const{
    int64_t start, end;

    bool found = pma_find(_PMA, min, &start);
    if(start == -1){ // All elements in the PMA are smaller than min
        start = 0;
    } else if (!found) { // start contains the predecessor. Move ahead of one position to ignore it
        start++;
    }

    found = pma_find(_PMA, max, &end);
    if(end == -1){ // All elements in the PMA are smaller than max
        end = 0;
    } else { // because end is not inclusive
        end++;
    }

    return std::pair<int64_t, int64_t>{start, end};
}

std::unique_ptr<pma::Iterator> PMA_Montes::find(int64_t min, int64_t max) const{
    auto interval = find_interval(min, max);

//    std::cout << "[PMA_Montes::find] min: " << min << ", max: " << max << ", start: " << start << ", end: " << end << std::endl;
    return std::unique_ptr<pma::Iterator>{ new detail_pabmont::IteratorImpl(_PMA, interval.first, interval.second) };
}

std::unique_ptr<pma::Iterator> PMA_Montes::iterator() const {
    std::unique_ptr<pma::Iterator> result { new detail_pabmont::IteratorImpl(_PMA, 0, pma_capacity(_PMA)) };
    return result;
}

pma::Interface::SumResult PMA_Montes::sum(int64_t min, int64_t max) const {
    auto interval = find_interval(min, max);
    auto start = interval.first;
    auto end = interval.second;

    SumResult sum;
    sum.m_first_key = sum.m_last_key = std::numeric_limits<int64_t>::min();

    // first element
    while(sum.m_first_key == std::numeric_limits<int64_t>::min() && start < end){
        pma_element_t element;
        pma_get(_PMA, start, &element);
        if(!keyval_empty(&element)){
            sum.m_first_key = element.key;
        } else {
            start++;
        }
    }

    // last element
    while(sum.m_last_key == std::numeric_limits<int64_t>::min() && start < end){
        pma_element_t element;
        pma_get(_PMA, end -1, &element);
        if(!keyval_empty(&element)){
            sum.m_last_key = element.key;
        } else {
            end--;
        }
    }

    // sum the elements in between
    for(auto i = start; i < end; i++){
        pma_element_t element;
        pma_get(_PMA, i, &element);
        if(!keyval_empty(&element)){
            sum.m_sum_keys += element.key;
            sum.m_sum_values += element.val;
            sum.m_num_elements++;
        }
    }

    return sum;
}

// For debugging purposes only
void PMA_Montes::dump() const {
    using namespace std;
    bool first = true;

    cout << "[";
    for(std::size_t i = 0, sz = capacity(); i < sz; i++){
        pma_element_t element = {-1, -1};
        pma_get(_PMA, i, &element);
        if(!keyval_empty(&element)){
            if(first){ first = false; } else { cout << ", "; }
            cout << "<" << element.key << ", " << element.val << ">";
        }
    }
    cout << "]\n";
}

void PMA_Montes::fulldump() const {
    using namespace std;

    for(std::size_t i = 0, sz = capacity(); i < sz; i++){
        pma_element_t element = {-1, -1};
        pma_get(_PMA, i, &element);
        cout << "[" << i << "] present: " << !keyval_empty(&element) << ", element: " << element.key << ", " << element.val << endl;
    }
}

#undef _PMA

} // namespace pma

/*****************************************************************************
 *                                                                           *
 *   Justin Raizes                                                           *
 *                                                                           *
 *****************************************************************************/
#include "external/raizes/pkd_mem_arr.h"

namespace pma {

// Copy & paste from pkd_mem_arr.c
/*
 * The following MACROS are courtesy of Professor Darrell Long
 * They were provided for an assigment in his class, and I have
 * seen fit to use them here.
 */
//# define    SETBIT(A, k) A[k >> 3] |=  (01 << (k & 07))
//# define    CLRBIT(A, k) A[k >> 3] &= ~(01 << (k & 07))
# define    GETBIT(A, k) (A[(k) >> 3] & (01 << ((k) & 07))) >> ((k) & 07)

#define _PMA reinterpret_cast<raizes_PMA*>(this->implementation)

PMA_Raizes::PMA_Raizes() : implementation(PMA_new()) {

}

PMA_Raizes::~PMA_Raizes() {
    PMA_free(_PMA);
    implementation = nullptr;
}

void PMA_Raizes::insert(int64_t key, int64_t value){
    PMA_insert(_PMA, key, value);
}

int64_t PMA_Raizes::find(int64_t key) const {
    int index = PMA_find(_PMA, key);
    if(index >= 0){
        return _PMA->array[index].value;
    } else {
        return -1;
    }
}

std::size_t PMA_Raizes::size() const {
    return PMA_get_count(_PMA);
}

namespace detail_raizes {

class IteratorImpl : public pma::Iterator {
    friend class ::pma::PMA_Raizes;
    void* implementation;
    int pos;

    IteratorImpl(void* impl);
    void move();

public :
    ~IteratorImpl();

    bool hasNext() const;
    std::pair<int64_t, int64_t> next();
};

IteratorImpl::IteratorImpl(void* impl): implementation(impl), pos(0){
    assert(impl != nullptr);
    move();
}

IteratorImpl::~IteratorImpl(){ }

void IteratorImpl::move() {
    char* __restrict bitmap = _PMA->bitmap;
    int sz = _PMA->size;
    while(pos < sz && (!(GETBIT(bitmap, pos))) ) pos++;
}

bool IteratorImpl::hasNext() const {
    return pos < _PMA->size;
}

std::pair<int64_t, int64_t> IteratorImpl::next() {
    assert(pos < _PMA->size && "Index out of bounds");
    assert((GETBIT((_PMA->bitmap), pos)) && "Invalid position, no element present");

    raizes_PMA_element e = _PMA->array[pos];
    auto result = std::make_pair(e.key, e.value);

    pos++; move(); // move to the next item

    return result;
}

} // namespace detail_raizes

std::unique_ptr<pma::Iterator> PMA_Raizes::iterator() const {
    std::unique_ptr<pma::Iterator> result(new detail_raizes::IteratorImpl(_PMA));
    return result;
}

pma::Interface::SumResult PMA_Raizes::sum(int64_t min, int64_t max) const {
    RAISE_EXCEPTION(Exception, "Method ::sum(int64_t min, int64_t max) not implemented!");
}


void PMA_Raizes::dump() const {
    PMA_print_content(_PMA);
}

#undef _PMA

} // namespace pma
