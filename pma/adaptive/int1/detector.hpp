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

#ifndef ADAPTIVE_INT1_DETECTOR_HPP_
#define ADAPTIVE_INT1_DETECTOR_HPP_

#include <cinttypes>
#include <cstddef>
#include <iostream>

namespace pma { namespace adaptive { namespace int1 {

struct Knobs;

class Detector {
private:
    Knobs& m_knobs; // apma settings
    int64_t* m_buffer = nullptr;
    uint64_t m_capacity;
    const uint32_t m_sizeof_entry; // the size of each entry, in terms of uint64_t multiples
    const uint32_t m_modulus; // number of recordings per segment
    int64_t m_counter =0; // internal counter

    template<int increment>
    void update(size_t segment_id, int64_t predecessor, int64_t successor);

    template<int increment>
    void update(size_t segment_id, int64_t predecessor, int64_t successor, int64_t timestamp);

public:
    Detector(Knobs& knobs, size_t size, uint32_t modulus);

    ~Detector();

    /**
     * Record an insertion
     */
    void insert(uint64_t segment, int64_t predecessor, int64_t successor);

    /**
     * Record a deletion
     */
    void remove(uint64_t segment, int64_t predecessor, int64_t successor);

    /**
     * Reset the recordings
     */
    void clear();

    /**
     * Reset the recordings of only a particular segment
     */
    void clear(uint64_t segment_id);

    /**
     * Alter the capacity of the underlying circular buffer. It automatically clears all previous recordings.
     */
    void resize(size_t size);

    /**
     * Dump the content of the data structure, for debug purposes.
     */
    void dump(std::ostream& out) const;
    void dump() const;

    size_t size() const;
    size_t capacity() const;
    int64_t* buffer() const;
    size_t sizeof_entry() const;
    size_t modulus() const;
};

}}} // pma::adaptive::int1

#endif /* ADAPTIVE_INT1_DETECTOR_HPP_ */
