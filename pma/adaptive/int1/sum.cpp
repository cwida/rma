/*
 * sum.cpp
 *
 *  Created on: 19 Aug 2018
 *      Author: Dean De Leo
 */

#include "sum.hpp"

#include <algorithm>
#include <cassert>
#include <iostream>

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace pma { namespace adaptive { namespace int1 {

::pma::Interface::SumResult do_sum(const Storage& storage, int64_t segment_start, int64_t segment_end, int64_t min, int64_t max){
    using SumResult = ::pma::Interface::SumResult;
    if(/* empty ? */storage.m_cardinality == 0 ||
       /* invalid min, max */ max < min ||
       /* wrong segments */ segment_end < segment_start){ return SumResult{}; }

    int64_t* __restrict keys = storage.m_keys;

    bool notfound = true;
    ssize_t segment_id = segment_start;
    bool segment_even = segment_id % 2 == 0;
    ssize_t start = -1, stop = -1, offset = -1;

    // start of the interval
    while(notfound && segment_id < storage.m_number_segments){
        if(segment_even){
            stop = (segment_id +1) * storage.m_segment_capacity;
            start = stop - storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, even segment, start: " << start << ", stop: " << stop);
        } else { // odd
            start = segment_id * storage.m_segment_capacity;
            stop = start + storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, odd segment, start: " << start << ", stop: " << stop);
        }
        offset = start;

        while(offset < stop && keys[offset] < min) {
            COUT_DEBUG("lower interval, offset: " << offset << ", key: " << keys[offset] << ", key_min: " << min);
            offset++;
        }

        notfound = (offset == stop);
        if(notfound){
            segment_id++;
            segment_even = !segment_even; // flip
        }
    }

    if(segment_even && segment_id < (storage.m_number_segments -1)){
        stop = (segment_id +1) * storage.m_segment_capacity + storage.m_segment_sizes[segment_id +1]; // +1 implicit
    }

    if(notfound || keys[offset] > max){ return SumResult{}; }

    ssize_t end;
    { // find the last qualifying index
        assert(segment_end < storage.m_number_segments);
        auto interval_start_segment = segment_id;
        ssize_t segment_id = segment_end;
        bool segment_even = segment_id % 2 == 0;
        notfound = true;
        ssize_t offset, start, stop;

        while(notfound && segment_id >= interval_start_segment){
            if(segment_even){
                start = (segment_id +1) * storage.m_segment_capacity -1;
                stop = start - storage.m_segment_sizes[segment_id];
            } else { // odd
                stop = segment_id * storage.m_segment_capacity;
                start = stop + storage.m_segment_sizes[segment_id] -1;
            }
            COUT_DEBUG("upper interval, " << (segment_even ? "even":"odd") << " segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
            offset = start;

            while(offset >= stop && keys[offset] > max){
                COUT_DEBUG("upper interval, offset: " << offset << ", key: " << keys[offset] << ", key_max: " << max);
                offset--;
            }

            notfound = offset < stop;
            if(notfound){
                segment_id--;
                segment_even = !segment_even; // flip
            }
        }

        end = offset +1;
    }

    if(end <= offset) return SumResult{};
    stop = std::min(stop, end);

    int64_t* __restrict values = storage.m_values;
    SumResult sum;
    sum.m_first_key = keys[offset];

    while(offset < end){
        sum.m_num_elements += (stop - offset);
        while(offset < stop){
            sum.m_sum_keys += keys[offset];
            sum.m_sum_values += values[offset];
            offset++;
        }

        segment_id += 1 + (segment_id % 2 == 0); // next even segment
        if(segment_id < storage.m_number_segments){
            ssize_t size_lhs = storage.m_segment_sizes[segment_id];
            ssize_t size_rhs = storage.m_segment_sizes[segment_id +1];
            offset = (segment_id +1) * storage.m_segment_capacity - size_lhs;
            stop = std::min(end, offset + size_lhs + size_rhs);
        }
    }
    sum.m_last_key = keys[end -1];

    return sum;
}

}}} // namespace pma::adaptive::int1
