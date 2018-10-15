/*
 * partition.hpp
 *
 *  Created on: Jul 10, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_BH07_V2_PARTITION_HPP_
#define ADAPTIVE_BH07_V2_PARTITION_HPP_

#include "../int2/partition.hpp"

namespace pma { namespace adaptive { namespace bh07_v2 {

using Partition = ::pma::adaptive::int2::Partition;
using VectorOfPartitions = ::pma::adaptive::int2::VectorOfPartitions;

/**
 * Obtain an empty vector of partitions
 */
inline VectorOfPartitions vector_of_partitions(CachedMemoryPool& memory_pool){
    return ::pma::adaptive::int2::vector_of_partitions(memory_pool);
}

}}} // pma::adaptive::bh07_v2

#endif /* ADAPTIVE_BH07_V2_PARTITION_HPP_ */
