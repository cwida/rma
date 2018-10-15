/*
 * iterator.hpp
 *
 *  Created on: 17 Aug 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_ITERATOR_HPP_
#define PMA_ITERATOR_HPP_

#include <cinttypes>
#include <memory>
#include <utility>

namespace pma {

/**
 * [Interface]
 * Java-like iterator (`enumerator' would be more appropriate). It provides
 * an interface to iterate over each element of the range.
 */
struct Iterator {
    virtual ~Iterator();
    virtual bool hasNext() const = 0;
    virtual std::pair<int64_t, int64_t> next() = 0;
};


} // namespace pma


#endif /* PMA_ITERATOR_HPP_ */
