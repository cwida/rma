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

#include "iterator.hpp"

#include <cassert>
#include <stdexcept>
#include "packed_memory_array.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int2 {

Iterator::Iterator(const Storage& storage) : m_pma(storage) { } // empty iterator

Iterator::Iterator(const Storage& storage, size_t segment_start, size_t segment_end, int64_t key_min, int64_t key_max) : m_pma(storage){
    if(segment_start > segment_end) throw invalid_argument("segment_start > segment_end");
    if(segment_end >= storage.m_number_segments) return;
    int64_t* __restrict keys = storage.m_keys;

    bool notfound = true;
    ssize_t segment_id = segment_start;
    bool segment_even = segment_id % 2 == 0;
    ssize_t start, stop = -1, offset = -1;

    while(notfound && segment_id < storage.m_number_segments){
        if(segment_even){
            stop = (segment_id +1) * storage.m_segment_capacity;
            start = stop - storage.m_segment_sizes[segment_id];
        } else { // odd
            start = segment_id * storage.m_segment_capacity;
            stop = start + storage.m_segment_sizes[segment_id];
        }
        offset = start;

        while(offset < stop && keys[offset] < key_min) {
            offset++;

        }

        notfound = (offset == stop);
        if(notfound){
            segment_id++;
            segment_even = !segment_even; // flip
        }
    }

    m_offset = offset;
    m_next_segment = segment_id +1;
    m_stop = stop;
    if(segment_even && m_next_segment < storage.m_number_segments){
        m_stop = m_next_segment * storage.m_segment_capacity + storage.m_segment_sizes[m_next_segment];
        m_next_segment++;
    }

    if(notfound || keys[m_offset] > key_max){
        m_index_max = m_stop = 0;
    } else {
        // find the last qualifying index
        assert(segment_end < storage.m_number_segments);
        auto interval_start_segment = segment_id;
        segment_id = segment_end;
        segment_even = segment_id % 2 == 0;
        notfound = true;

        while(notfound && segment_id >= interval_start_segment){
            if(segment_even){
                start = (segment_id +1) * storage.m_segment_capacity -1;
                stop = start - storage.m_segment_sizes[segment_id];
            } else { // odd
                stop = segment_id * storage.m_segment_capacity;
                start = stop + storage.m_segment_sizes[segment_id] -1;
            }
            offset = start;

            while(offset >= stop && keys[offset] > key_max) { offset--; }

            notfound = offset < stop;
            if(notfound){
                segment_id--;
                segment_even = !segment_even; // flip
            }
        }

        if(offset < static_cast<ssize_t>(m_offset)){
            m_index_max = m_stop = 0;
        } else {
            m_index_max = offset+1;
            m_stop = min(m_index_max, m_stop);
        }
    }
}

void Iterator::next_sequence() {
    assert(m_offset >= m_stop);
    size_t segment1 = m_next_segment;

    if(segment1 < m_pma.m_number_segments){
        bool segment_even = segment1 % 2 == 0;
        if(segment_even){
            m_offset = segment1 * m_pma.m_segment_capacity + m_pma.m_segment_capacity - m_pma.m_segment_sizes[segment1];
            auto segment2 = segment1 +1;
            m_stop = segment2 * m_pma.m_segment_capacity;
            if(segment2 < m_pma.m_number_segments){
                m_stop = min(m_stop + m_pma.m_segment_sizes[segment2], m_index_max);
            } else {
                m_stop = min(m_stop, m_index_max);
            }

            m_next_segment += 2;
        } else { // odd segment
            m_offset = segment1 * m_pma.m_segment_capacity;
            m_stop = min(m_index_max, m_offset + m_pma.m_segment_sizes[segment1]);
            m_next_segment++;
        }
    }
}

bool Iterator::hasNext() const {
    return m_offset < m_stop;
}

std::pair<int64_t, int64_t> Iterator::next() {
    int64_t* keys = m_pma.m_keys;
    int64_t* values = m_pma.m_values;

    pair<int64_t, int64_t> result { keys[m_offset], values[m_offset] };

    m_offset++;
    if(m_offset >= m_stop) next_sequence();

    return result;
}

}}} // pma::adaptive::int2
