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

#include "move_detector_info.hpp"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include "detector.hpp"
#include "memory_pool.hpp"
#include "packed_memory_array.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

MoveDetectorInfo::MoveDetectorInfo(PackedMemoryArray& pma, size_t segment_start)
    : MoveDetectorInfo(pma.memory_pool(), pma.detector().buffer() + segment_start * pma.detector().sizeof_entry(),
            pma.detector().sizeof_entry()) { }

MoveDetectorInfo::MoveDetectorInfo(CachedMemoryPool& memory_pool, int64_t* detector_buffer, const size_t entry_size) :
        m_memory_pool(memory_pool), m_detector_buffer(detector_buffer), m_detector_entry_size(entry_size),
        m_registered_segments(nullptr), m_registered_segments_capacity(0), m_registered_segments_sz(0) {

}

MoveDetectorInfo::~MoveDetectorInfo(){
    move();
    m_memory_pool.deallocate(m_registered_segments); m_registered_segments = nullptr;
}


void MoveDetectorInfo::resize(size_t capacity){
    if(capacity < m_registered_segments_sz){ throw invalid_argument("Cannot reduce the size of the internal buffer"); }
    decltype(m_registered_segments) registered_segments_new = m_memory_pool.allocate<remove_pointer_t<decltype(m_registered_segments)>>(capacity);
    if(m_registered_segments != nullptr){
        memcpy(registered_segments_new, m_registered_segments, sizeof(m_registered_segments[0]) * m_registered_segments_sz);
        m_memory_pool.deallocate(m_registered_segments);
    }
    m_registered_segments = registered_segments_new;
    m_registered_segments_capacity = capacity;
}

void MoveDetectorInfo::move_section(uint32_t from, uint32_t to){
    if(m_registered_segments_sz >= m_registered_segments_capacity){ throw runtime_error("[MoveDetectorInfo::register_section] No space left"); }
    if(from != to) // otherwise it's not moving anything
        m_registered_segments[m_registered_segments_sz++] = {from, to};
}

void MoveDetectorInfo::move(){
    auto& memory_pool = m_memory_pool;
    auto fn_deallocate = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(fn_deallocate)> buffer_ptr = { m_memory_pool.allocate<int64_t>(sizeof(int64_t) * m_detector_entry_size * m_registered_segments_sz), fn_deallocate };
    int64_t* buffer = buffer_ptr.get();
    const size_t detector_entry_size_bytes = sizeof(int64_t) * m_detector_entry_size;

    for(size_t i = 0; i < m_registered_segments_sz; i++){
        memcpy(buffer + i * m_detector_entry_size, m_detector_buffer + m_registered_segments[i].first * m_detector_entry_size, detector_entry_size_bytes);
        memset(m_detector_buffer + m_registered_segments[i].first * m_detector_entry_size, 0, detector_entry_size_bytes);
    }

    for(size_t i = 0; i < m_registered_segments_sz; i++){
        memcpy(m_detector_buffer + m_registered_segments[i].second * m_detector_entry_size, buffer + i * m_detector_entry_size, detector_entry_size_bytes);
    }
}


void MoveDetectorInfo::dump(std::ostream& out) const{
    out << "{MoveDetectorInfo entry size: " << m_detector_entry_size << ", size: " << m_registered_segments_sz << ", capacity: " <<
            m_registered_segments_capacity << ", entries: [";
    for(size_t i = 0; i < m_registered_segments_sz; i++){
        if(i>0) out << ", ";
        out << m_registered_segments[i].first << " -> " << m_registered_segments[i].second;
    }
    out << "]}";
}

void MoveDetectorInfo::dump() const{
    dump(cout);
    cout << endl;
}

ostream& operator<<(ostream& out, const MoveDetectorInfo& mdi){
    mdi.dump(out);
    return out;
}


}}} // pma::adaptive::int1


