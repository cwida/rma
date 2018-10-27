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

#ifndef ADAPTIVE_INT1_PARTITION_HPP_
#define ADAPTIVE_INT1_PARTITION_HPP_

#include <cinttypes>
#include <ostream>
#include <vector>

#include "memory_pool.hpp"

namespace pma { namespace adaptive { namespace int1 {

struct Partition {
    uint32_t m_cardinality; // total amount of elements
    uint32_t m_segments; // number of segments

    Partition();

    template<typename T1, typename T2>
    Partition(T1 cardinality, T2 segments)
        : m_cardinality( static_cast<decltype(m_cardinality)>(cardinality) ),
          m_segments( static_cast<decltype(m_segments)>(segments) ){

    }
};


std::ostream& operator<<(std::ostream& out, Partition p);


/**
 * A vector of Partitions, managed through the custom allocator CachedAllocator
 */
using VectorOfPartitions = std::vector<Partition, CachedAllocator<Partition>>;

/**
 * Obtain an empty vector of partitions
 */
VectorOfPartitions vector_of_partitions(CachedMemoryPool& memory_pool);

}}} // pma::adaptive::int1



#endif /* ADAPTIVE_INT1_PARTITION_HPP_ */
