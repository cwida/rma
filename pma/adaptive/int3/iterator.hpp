/*
 * iterator.hpp
 *
 *  Created on: 9 Sep 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_ADAPTIVE_INT3_ITERATOR_HPP_
#define PMA_ADAPTIVE_INT3_ITERATOR_HPP_

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

#include "storage.hpp"

namespace pma { namespace adaptive { namespace int3 {

class Iterator : public ::pma::Iterator {
    const Storage& m_pma;
    size_t m_next_segment = 0;
    size_t m_offset = 0;
    size_t m_stop = 0; // index when the current sequence stops
    size_t m_index_max = 0;

    void next_sequence(); // update m_offset and m_stop to point to the next qualifying sequence

public:
    Iterator(const Storage& storage); // empty iterator
    Iterator(const Storage& storage, size_t segment_start, size_t segment_end, int64_t key_min, int64_t key_max);

    virtual bool hasNext() const;
    virtual std::pair<int64_t, int64_t> next();
};


}}} // pma::adaptive::int3

#endif /* PMA_ADAPTIVE_INT3_ITERATOR_HPP_ */
