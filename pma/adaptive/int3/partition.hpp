/*
 * partition.hpp
 *
 *  Created on: 9 Sep 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_ADAPTIVE_INT3_PARTITION_HPP_
#define PMA_ADAPTIVE_INT3_PARTITION_HPP_

#include "../int1/partition.hpp"

namespace pma { namespace adaptive { namespace int3 {

using Partition = ::pma::adaptive::int1::Partition;
using VectorOfPartitions = ::pma::adaptive::int1::VectorOfPartitions;

/**
 * Obtain an empty vector of partitions
 */
inline VectorOfPartitions vector_of_partitions(CachedMemoryPool& memory_pool){
    return ::pma::adaptive::int1::vector_of_partitions(memory_pool);
}

}}} // pma::adaptive::int3

#endif /* PMA_ADAPTIVE_INT3_PARTITION_HPP_ */
