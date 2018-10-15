/*
 * storage.cpp
 *
 *  Created on: Mar 7, 2018
 *      Author: dleo@cwi.nl
 */

#include "storage.hpp"

#include <cmath>
#include <numeric>
#include <stdexcept>
#include "errorhandling.hpp"
#include "miscellaneous.hpp" // hyperceil

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

Storage::Storage(size_t segment_size) : m_segment_capacity( hyperceil(segment_size ) ){
    if(hyperceil(segment_size ) > numeric_limits<uint16_t>::max()) throw invalid_argument("segment size too big, maximum is " + to_string( numeric_limits<uint16_t>::max() ));
    if(m_segment_capacity < 8) throw invalid_argument("segment size too small, minimum is 8");

    m_capacity = m_segment_capacity;
    m_number_segments = 1;
    m_height = 1;
    m_cardinality = 0;

    // memory allocations
    alloc_workspace(1, &m_keys, &m_values, &m_segment_sizes);
}

void Storage::alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes){
    // reset the ptrs
    *keys = nullptr;
    *values = nullptr;
    *sizes = nullptr;

    int rc(0);
    rc = posix_memalign((void**) keys, /* alignment */ 64,  /* size */ num_segments * m_segment_capacity * sizeof(m_keys[0]));
    if(rc != 0) {
        RAISE_EXCEPTION(Exception, "[Storage::Storage] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << m_segment_capacity * sizeof(m_keys[0]));
    }
    rc = posix_memalign((void**) values, /* alignment */ 64,  /* size */ num_segments * m_segment_capacity * sizeof(m_values[0]));
    if(rc != 0) {
        free(*keys); *keys = nullptr;
        RAISE_EXCEPTION(Exception, "[Storage::Storage] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << m_segment_capacity * sizeof(m_values[0]));
    }

    rc = posix_memalign((void**) sizes, /* alignment */ 64,  /* size */ num_segments * sizeof(m_segment_sizes[0]));
    if(rc != 0) {
        free(*keys); *keys = nullptr;
        free(*values); *values = nullptr;
        RAISE_EXCEPTION(Exception, "[Storage::Storage] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << m_segment_capacity * sizeof(m_segment_sizes[0]));
    }
}

}}} // pma::adaptive::int1


