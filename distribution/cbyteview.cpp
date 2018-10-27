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

