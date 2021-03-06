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
