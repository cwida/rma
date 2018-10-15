/*
 * adaptive_rebalancing.hpp
 *
 *  Created on: Jul 10, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_BH07_V2_ADAPTIVE_REBALANCING_HPP_
#define ADAPTIVE_BH07_V2_ADAPTIVE_REBALANCING_HPP_

#include <cinttypes>
#include <cstddef>
#include <vector>

#include "partition.hpp"

namespace pma { namespace adaptive { namespace bh07_v2 {

class APMA_BH07_v2; // forward decl.
struct PermutedItem; // forward decl.

class AdaptiveRebalancing {
public:
    /**
     * Retrieve the number of elements to allocate in each partition, according to the current state of the
     * predictor and the `adaptive' strategy
     */
    static VectorOfPartitions partitions(APMA_BH07_v2* instance, int height, size_t cardinality, size_t segment_start, size_t num_segments, bool resize, bool can_fill_segments);

private:
    /**
     * Recursive step, compute the left & right cardinality for the given window
     */
    static void compute_rec(APMA_BH07_v2* instance, VectorOfPartitions& partitions, int64_t partitions_base, int height, size_t cardinality, size_t segment_start, size_t num_segments, const std::vector<PermutedItem>& weights, size_t weights_start, size_t weights_length, bool resize, bool can_fill_segments);

};

}}} // pma::adaptive::bh07_v2

#endif /* ADAPTIVE_BH07_V2_ADAPTIVE_REBALANCING_HPP_ */
