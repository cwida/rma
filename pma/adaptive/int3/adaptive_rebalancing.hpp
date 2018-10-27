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

#ifndef PMA_ADAPTIVE_INT3_ADAPTIVE_REBALANCING_HPP_
#define PMA_ADAPTIVE_INT3_ADAPTIVE_REBALANCING_HPP_


#include <cinttypes>
#include <ostream>
#include <utility>
#include <vector>
#include "partition.hpp"
#include "weights.hpp"

// forward decl.
namespace pma { struct DensityBounds; }

namespace pma { namespace adaptive { namespace int3 {

// forward decl.
class MoveDetectorInfo;
class PackedMemoryArray;

struct Optimum {
    int m_cardinality;
    int m_weights_index;
    int m_weights_balance;

    Optimum();
    Optimum(int cardinality);
    Optimum(int cardinality, int weights_index, int weights_balance);
};

std::ostream& operator<<(std::ostream& out, Optimum opt);

class AdaptiveRebalancing {
private:
    VectorOfIntervals m_weights;
//    const size_t m_partitions_length;
    const size_t m_height; // the height of the calibrator tree
    const size_t m_segment_capacity; // the capacity of each segment
    const DensityBounds& m_densities;
    MoveDetectorInfo* m_ptr_move_detector_info;

    bool m_output_released = false; // Whether the final partitions have been computed
    VectorOfPartitions m_partitions; // the output to compute

    struct FindSplitPointResult{ int m_left_index; int m_left_balance; };
    FindSplitPointResult find_split_point(Interval* weights, size_t weights_sz, int balance);

//    //** FOR DEBUG ONLY
//    uint16_t* m_debug_segment_sizes = nullptr;
//    uint64_t m_debug_window_start = 0;
//    uint64_t m_debug_window_length = 0;

    void move_detector_info(int segment_id, int destination);

    /**
     * Find the optimum point using just in the middle between weights[index_split] and weights[index_split +1]
     */
    int rebalancing_paro(Interval* weights, size_t weights_sz, int index_split, size_t cardinality);

    /**
     * Find the optimum point with an odd number of weights
     */
    int rebalancing_sparu(Interval* weights, size_t weights_sz, int index_split, size_t cardinality);

    // Find the optimum partitions, regardless of the lower & upper thresholds
    Optimum find_optimum(Interval* weights, size_t weights_length, int balance, size_t cardinality);

    // Ensure the optimum split point is within the lower & upper thresholds
    Optimum validate_thresholds(size_t part_length, Interval* weights, size_t weights_length, int balance, size_t cardinality, Optimum opt);

    Optimum ensure_lower_threshold(size_t left_cardinality_min, size_t left_cardinality_max, Interval* weights, size_t weights_length, int balance, Optimum current);
    Optimum ensure_upper_threshold(size_t left_cardinality_min, size_t left_cardinality_max, Interval* weights, size_t weights_length, int balance, Optimum current);

    // Define the cardinality for the next section, to be evenly distributed among `number_of_segments'
    void emit(size_t cardinality, size_t number_of_segments);

    void recursion(size_t part_start, size_t part_length, Interval* weights, size_t weights_length, int balance, size_t cardinality);

    /**
     * Get the lower & higher threshold in the calibrator tree for the node at the given height
     */
    std::pair<double, double> get_density(double height);

public:
    AdaptiveRebalancing(PackedMemoryArray& pma, VectorOfIntervals weights, int balance, size_t num_partitions, size_t cardinality, MoveDetectorInfo* ptr_move_detector_info);

    ~AdaptiveRebalancing();

    // Dump the computed partition, for debug only
//    void set_debug_info(uint16_t* segment_sizes, uint64_t window_start, size_t window_length);
    void dump(std::ostream& out) const;
    void dump() const;

    VectorOfPartitions release();
};

}}} // pma::adaptive::int3

#endif /* PMA_ADAPTIVE_INT3_ADAPTIVE_REBALANCING_HPP_ */
