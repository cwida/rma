/*
 * partition.hpp
 *
 *  Created on: 4 Jul 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_INT2_PARTITION_HPP_
#define ADAPTIVE_INT2_PARTITION_HPP_

#include "../int1/partition.hpp"

namespace pma { namespace adaptive { namespace int2 {

using Partition = ::pma::adaptive::int1::Partition;
using VectorOfPartitions = ::pma::adaptive::int1::VectorOfPartitions;

/**
 * Obtain an empty vector of partitions
 */
inline VectorOfPartitions vector_of_partitions(CachedMemoryPool& memory_pool){
    return ::pma::adaptive::int1::vector_of_partitions(memory_pool);
}

}}} // pma::adaptive::int2

#endif /* ADAPTIVE_INT2_PARTITION_HPP_ */
