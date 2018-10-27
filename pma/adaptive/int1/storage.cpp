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


