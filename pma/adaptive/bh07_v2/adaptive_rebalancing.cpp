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

#include "adaptive_rebalancing.hpp"

#include <cassert>
#include <cmath>
#include <iostream>

#include "packed_memory_array.hpp"
#include "partition.hpp"
#include "predictor.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[AdaptiveRebalancing::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
    #define IFDEBUG(op) op
#else
    #define COUT_DEBUG(msg)
    #define IFDEBUG(op)
#endif


namespace pma { namespace adaptive { namespace bh07_v2 {

VectorOfPartitions AdaptiveRebalancing::partitions(APMA_BH07_v2* instance, int height, size_t cardinality, size_t window_start, size_t window_length, bool resize, bool can_fill_segments){
    assert(cardinality > 0 && "Are we resizing an empty array?");
    assert(instance != nullptr && "Null pointer");

    // find the min and max of the required region
    size_t min = window_start;
    size_t max = min + window_length -1;

    auto weights = instance->m_predictor.items(min, max);
    VectorOfPartitions partitions = vector_of_partitions(instance->memory_pool()); // result

    // as we are going to double the size of the array, adjust where the weights are going to point
    if (resize){
        for(size_t i = 0, end = weights.size(); i < end; i++){
            instance->m_predictor.reset_ptr(weights[i].m_permuted_position, weights[i].m_pointer *2);
        }
    }

#if defined(DEBUG)
    COUT_DEBUG("height: " << height << ", cardinality: " << cardinality << ", segment_start: " << window_start << ", segment_length: " << window_length);
    COUT_DEBUG("weights size: " << weights.size() << ", min: " << min << ", max: " << max);
    for(size_t i = 0; i < weights.size(); i++){
        cout << "[" << i << "] " << weights[i] << "\n";
    }
    flush(cout);
#endif

    compute_rec(instance, partitions, -window_start, height, cardinality, window_start, window_length, weights, 0, weights.size(), resize, can_fill_segments);

#if !defined(NDEBUG)
    { // restrict the scope
        COUT_DEBUG("partitions: " << partitions.size());
        size_t sum = 0;
        size_t segment_id = 0; // current segment
        for(auto& partition : partitions){
            size_t card_per_segment = partition.m_cardinality / partition.m_segments;
            size_t odd_segments = partition.m_cardinality % partition.m_segments;
            for(size_t j = 0; j < partition.m_segments; j++){
                size_t segment_cardinality = (card_per_segment + (j < odd_segments));
                COUT_DEBUG("[" << (window_start + segment_id) << "] cardinality: " << segment_cardinality);
                segment_id++;
                sum += segment_cardinality;
            }
        }
        assert(segment_id = window_length);
        assert(sum == cardinality);
    }
#endif
    return partitions;
}

void AdaptiveRebalancing::compute_rec(APMA_BH07_v2* instance, VectorOfPartitions& partitions, int64_t partitions_base_adjustment, int height, size_t cardinality, size_t segment_start, size_t num_segments, const vector<PermutedItem>& weights, size_t weights_start, size_t weights_length, bool resize, bool can_fill_segments){
    COUT_DEBUG("--- apma_compute_partitions_rec ---");
    COUT_DEBUG("segment_start: " << segment_start << ", segment_length: " << num_segments << ", cardinality: " << cardinality <<
            ", height: " << height << ", weights_start: " << weights_start << ", weights_length: " << weights_length);

#if !defined(NDEBUG) // sanity check
    {
        auto bounds = instance->thresholds(height);
        double wrho = bounds.first, wtheta = bounds.second;
        size_t wcapacity = num_segments * instance->m_storage.m_segment_capacity;
        assert(wcapacity * wrho <= cardinality);
        assert(cardinality <= wcapacity * wtheta);
    }
#endif

    if(num_segments == 1){ // base case
        COUT_DEBUG("base case, segment: " << segment_start << ", cardinality: " << cardinality);
        assert(cardinality <= instance->m_storage.m_segment_capacity);
        partitions.push_back({cardinality, 1});
    } else if ((weights_length == 0) || (resize && num_segments == 2)){ // even rebalancing
        COUT_DEBUG("even rebalancing");
        partitions.push_back({cardinality, num_segments});
    } else { // uneven rebalancing
        COUT_DEBUG("uneven rebalancing");
        const size_t segment_capacity = instance->m_storage.m_segment_capacity;

        int height_children = height -1;
        assert(height_children >= 1);
        auto bounds = instance->thresholds(height_children);
        double rho = bounds.first, theta = bounds.second;
        size_t num_segments_children = pow(2.0, height_children -1);
        size_t capacity_children = segment_capacity * num_segments_children;
        COUT_DEBUG("capacity_children: " << capacity_children);

        size_t size_min = ceil(rho * capacity_children);
        size_t size_max = theta * capacity_children;
        if(!can_fill_segments){
            // ensure that at least one slot per segment will remain free
            const size_t max_window_cardinality = capacity_children - num_segments_children;
            size_max = min<size_t>(size_max, max_window_cardinality);
        }
        size_t fill_min = max<int64_t>(0, static_cast<int64_t>(cardinality) - size_max); // avoid a value less than 0
        size_t fill_max = cardinality - size_min;

        size_t index_start = segment_capacity * segment_start; // inclusive
        //        size_t index_end = index_start + num_segments * m_storage.m_segment_capacity; // exclusive

        size_t start = max(size_min, fill_min);
        size_t end = min(size_max, fill_max);
        COUT_DEBUG("size_min: " << size_min << ", size_max: " << size_max << ", fill_min: " << fill_min << ", fill_max: " << fill_max << ", start: " << start << ", end: " << end);

        double denom_left = capacity_children - start;
        double denom_right = capacity_children - (cardinality - start);

        COUT_DEBUG("starting denom_left: " << denom_left << ", denom_right: " << denom_right);

        size_t sum_num_left = 0;
        size_t sum_num_right = 0;

        size_t weights_pos = weights_start;
        size_t weights_end = weights_start + weights_length;

        size_t index_opt = index_start + start;
        size_t splitnum = start;

        while(weights_pos < weights_end && (weights[weights_pos].m_pointer * segment_capacity) < index_opt) {
            sum_num_left += weights[weights_pos].m_count;
            weights_pos++;
        }
        size_t weights_next_index, weights_next_ptr;
        if(weights_pos < weights_end){
            weights_next_index = weights_pos;
            weights_next_ptr = (weights[weights_next_index].m_pointer * segment_capacity) +1;
        } else {
            weights_next_index = 0;
            weights_next_ptr = numeric_limits<size_t>::max();
        }
        size_t weights_split =  weights_pos - weights_start;

        while(weights_pos < weights_end){
            sum_num_right += weights[weights_pos].m_count;
            weights_pos++;
        }
        COUT_DEBUG("starting weights_split: " << weights_split << ", sum left: " << sum_num_left << ", sum right: " << sum_num_right);

        double optvalue = abs( (((double) sum_num_left) / denom_left) - (((double) sum_num_right) / max(1.0, denom_right)) );
        COUT_DEBUG("[" << start << "] starting optvalue: " << optvalue);


        for(size_t i = start +1; i <= end; i++){
            denom_left--;
            denom_right++;
            size_t index_cur = index_start + i;
            if(index_cur == weights_next_ptr){
                size_t count = weights[weights_pos].m_count;
                sum_num_left += count;
                sum_num_right -= count;
                weights_next_index++;
                if(weights_next_index < weights_end){
                    weights_next_ptr = (weights[weights_next_index].m_pointer * segment_capacity) +1;
                } else {
                    weights_next_ptr = numeric_limits<size_t>::max();
                }
            }

            double curvalue = abs( (((double) sum_num_left) / max(1.0, denom_left)) - (((double) sum_num_right) / max(1.0, denom_right)) );
            COUT_DEBUG("[" << i << "] curvalue: " << curvalue << ", num elements left: " << i << ", num elements right: " << cardinality - i);

            if(curvalue < optvalue){
                optvalue = curvalue;
                splitnum = i;
                index_opt = index_cur;
                weights_split = weights_next_index - weights_start;
            }

        }

        size_t cardinality_left = splitnum;
        size_t cardinality_right = cardinality - splitnum;

        COUT_DEBUG("index_opt: " << index_opt << ", optvalue: " << optvalue << ", weights_split: " << weights_split);
        COUT_DEBUG("num elements left: " << cardinality_left << ", num elements right: " << cardinality_right);

        // recursive step
        size_t segment_split = num_segments / 2;
        compute_rec(instance, partitions, partitions_base_adjustment, height -1, cardinality_left, segment_start, segment_split, weights, weights_start, weights_split, resize, can_fill_segments);
        compute_rec(instance, partitions, partitions_base_adjustment, height -1, cardinality_right, segment_start + segment_split, num_segments - segment_split, weights, weights_split, weights_length - weights_split, resize, can_fill_segments);
    }
}

}}} // pma::adaptive::bh07_v2
