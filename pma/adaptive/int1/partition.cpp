/*
 * partition.cpp
 *
 *  Created on: Mar 7, 2018
 *      Author: dleo@cwi.nl
 */

#include "partition.hpp"

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

Partition::Partition() : Partition(0,0) { }

ostream& operator<<(ostream& out, Partition p){
    out << "{PART cardinality: " << p.m_cardinality << ", segments: " << p.m_segments << "}";
    return out;
}

VectorOfPartitions vector_of_partitions(CachedMemoryPool& memory_pool){
    return VectorOfPartitions{ memory_pool.allocator<Partition>() };
}

}}} // pma::adaptive::int1
