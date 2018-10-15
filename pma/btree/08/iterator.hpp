/*
 * iterator.hpp
 *
 *  Created on: Sep 6, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef BTREE_08_ITERATOR_HPP_
#define BTREE_08_ITERATOR_HPP_

#include "pma/iterator.hpp"

namespace pma { namespace v8 {

class Storage; // forward decl.

class Iterator : public pma::Iterator {
    const Storage& m_storage;
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
} /* namespace v8 */
} /* namespace pma */

#endif /* BTREE_08_ITERATOR_HPP_ */
