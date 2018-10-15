/*
 * cbyteview.cpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#include "cbyteview.hpp"

#include <cassert>
#include <iostream> // debug only
#include <stdexcept>

using namespace std;

namespace distribution {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[CByteView::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   IMPLEMENTATION                                                          *
 *                                                                           *
 *****************************************************************************/

CByteView::CByteView(std::shared_ptr<CByteArray> container) : CByteView(container, 0, container->capacity()) { }

CByteView::CByteView(std::shared_ptr<CByteArray> container, size_t begin, size_t end) : container_ptr(container), container(container.get()), begin(begin), end(end), dense(false) {
    COUT_DEBUG("begin: " << begin << ", end: " << end);
}

size_t CByteView::size() const {
    return end - begin;
}

KeyValue CByteView::get(size_t index) const {
    int64_t k = key(index);
    return {k, k*10};
}

int64_t CByteView::key(size_t index) const {
    assert(index < size() && "Index out of bound");
    COUT_DEBUG("begin: " << begin << ", index: " << index);
    return container->get_value_at(begin + index) +1; // 0 -> 1, ... 1G -> 1G +1
}

void CByteView::sort() {
    container->sort(begin, end);
}

std::unique_ptr<Distribution> CByteView::view(size_t start, size_t length) {
    start += begin;
    if(start >= container->capacity()) throw std::invalid_argument("start >= capacity()");
    size_t end = start + length;
    if(end > container->capacity()) throw std::invalid_argument("start + length > capacity()");
    return make_unique<CByteView>(container_ptr, start, end);
}

bool CByteView::is_dense() const {
    return dense;
}

void CByteView::set_dense(bool value){
    dense = value;
}

} // namespace distribution

