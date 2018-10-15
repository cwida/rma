/*
 * partition.hpp
 *
 *  Created on: Mar 7, 2018
 *      Author: dleo@cwi.nl
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
