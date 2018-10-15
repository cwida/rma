/*
 * adaptive_rebalancing.cpp
 *
 *  Created on: Mar 6, 2018
 *      Author: dleo@cwi.nl
 */

#include "adaptive_rebalancing.hpp"

#include <cmath>
#include <iomanip>
#include <iostream>
#include "pma/density_bounds.hpp"
#include "errorhandling.hpp"
#include "move_detector_info.hpp"
#include "packed_memory_array.hpp"
#include "partition.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "\t[" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace pma { namespace adaptive { namespace int2 {

AdaptiveRebalancing::AdaptiveRebalancing(PackedMemoryArray& pma, VectorOfIntervals weights, int balance, size_t num_partitions, size_t cardinality,
        MoveDetectorInfo* ptr_move_detector_info, bool can_fill_segments) :
    m_weights(weights), /*m_partitions_length(num_partitions),*/ m_height(log2(num_partitions) +1),
    m_segment_capacity(pma.get_segment_capacity()), m_densities(pma.densities()),
    m_ptr_move_detector_info(ptr_move_detector_info),
    m_partitions{ vector_of_partitions(pma.memory_pool()) },
    m_can_fill_segments(can_fill_segments)
    {
    m_partitions.reserve(2 * m_weights.size() +1);

    recursion(0, num_partitions, m_height, m_weights.data(), m_weights.size(), balance, cardinality);
}

AdaptiveRebalancing::~AdaptiveRebalancing() { }

VectorOfPartitions AdaptiveRebalancing::release() {
#if defined(DEBUG)
    dump();
#endif

    if(m_output_released) RAISE_EXCEPTION(Exception, "Vector already released!");
    m_output_released = true;
    return std::move(m_partitions);
}

std::pair<double, double> AdaptiveRebalancing::get_density(int node_height){
    return m_densities.thresholds(node_height);
}

void AdaptiveRebalancing::emit(size_t cardinality, size_t number_of_segments){
    assert( ((m_can_fill_segments && cardinality <= number_of_segments * m_segment_capacity) ||
            (!m_can_fill_segments && cardinality <= number_of_segments * (m_segment_capacity -1))) && "Overflow");
    m_partitions.push_back({cardinality, number_of_segments});
}

void AdaptiveRebalancing::move_detector_info(int segment_id, int destination){
    if(segment_id >= 0 && m_ptr_move_detector_info){
        m_ptr_move_detector_info->move_section(segment_id, destination);
    }
}

AdaptiveRebalancing::FindSplitPointResult
AdaptiveRebalancing::find_split_point(Interval* weights, size_t weights_sz, int balance_total){
    assert(weights_sz > 0 && "Empty array weights");

    int i = 0;
    int j = weights_sz -1;
    int target = balance_total / 2;
    int balance_left = 0;
    int balance_right = 0;
    bool move_left = true;

    while (i <= j){
        if(move_left){
            do {
                balance_left += weights[i].m_weight;
                i++;
            } while(balance_left != target && i <= j);
            move_left = false;
        } else {
            do {
                balance_right += weights[j].m_weight;
                j--;
            } while(balance_right != target && i <= j);
            move_left = true;
        }
    }

    COUT_DEBUG("i: " << i << ", balance_left: " << balance_left << ", move_left: " << move_left);

    // retract the last move
    if(!move_left){
        i--;
    } else {
        j++;
    }

//    assert(balance_left + balance_right == balance_total);

    return { i, balance_left };
}


int AdaptiveRebalancing::rebalancing_paro(Interval* weights, size_t weights_sz, int index_split, size_t cardinality){
    COUT_DEBUG("weights_sz: " << weights_sz << ", index_split: " << index_split);

    if(index_split < 0){
        return (weights[0].m_start) /2;
    } else  {
        int base_left = weights[index_split].m_start + weights[index_split].m_length;
        if(index_split +1 == weights_sz){
            return base_left + cardinality /2;
        } else {
            int base_right = weights[index_split +1].m_start;
            assert(base_left <= base_right);
            return base_left + (base_right - base_left) /2;
        }
    }
}

int AdaptiveRebalancing::rebalancing_sparu(Interval* weights, size_t weights_sz, int index_split, size_t cardinality){
    assert(index_split >= 0 && index_split < weights_sz && "Index out of bounds");
//    int segment = candidates[index_split].m_segment_id;
    int weight = weights[index_split].m_weight;
    size_t card_left = weights[index_split].m_start + weights[index_split].m_length;
    size_t card_right = cardinality - (weights[index_split].m_start);
    size_t card_opt_left (0);
    if(weight < 0){
        if(card_left < card_right){ // put on the right
            card_opt_left = weights[index_split].m_start;
        } else { // put on the left
            card_opt_left = card_left;
        }
    } else { // weight > 0
        if(card_left < card_right){ // put on the left
            card_opt_left = card_left;
        } else { // put on the right
            card_opt_left = weights[index_split].m_start;
        }
    }

    return card_opt_left;
}

Optimum AdaptiveRebalancing::find_optimum(Interval* W, size_t W_sz, int balance, size_t cardinality){
    auto split_point = find_split_point(W, W_sz, balance);

    COUT_DEBUG("split point: " << split_point.m_left_index << ", balance: " << split_point.m_left_balance);

    int card_left = -1;
    if(W_sz % 2 == 0) { // rebalancing paro
        card_left = rebalancing_paro(W, W_sz, split_point.m_left_index, cardinality);
        COUT_DEBUG("rebalancing_paro left: " << card_left << "/" << cardinality);
    } else { // rebalancing_sparu
        card_left = rebalancing_sparu(W, W_sz, split_point.m_left_index, cardinality);
        COUT_DEBUG("rebalancing_sparu left: " << card_left << "/" << cardinality);

        // did it put the split point on the right?
        if(card_left < W[split_point.m_left_index].m_start){
            split_point.m_left_balance -= W[split_point.m_left_index].m_weight;
            split_point.m_left_index--;
        }
    }

    return Optimum{card_left, split_point.m_left_index, split_point.m_left_balance};
}

Optimum AdaptiveRebalancing::ensure_lower_threshold(size_t left_cardinality_min, size_t left_cardinality_max, Interval* weights, size_t weights_length, int balance, Optimum current){
    int objective = left_cardinality_min;
    int idx_split = current.m_weights_index;
    int weight_balance = current.m_weights_balance;
    COUT_DEBUG("init, objective: " << objective << ", idx_split: " << idx_split << ", weight_balance: " << weight_balance);

    // CORNER CASE, idx_split == -1
    if(idx_split == -1){ // bloody corner case when there are no weights considered for the left interval
        assert(weights_length > 0 && "Otherwise why are we muddling in adaptive rebalancing?");
        COUT_DEBUG("idx_split = -1");
        int64_t w_start = weights[0].m_start; // inclusive
        int64_t w_end = w_start + weights[0].m_length; // exclusive
        if(w_end < objective){ // ok, move ahead of the corner case
            idx_split = 0;
            weight_balance += weights[0].m_weight;

        } else if(w_start < objective){ // we have w_start < target < w_len, we need to split at this interval
            if(w_end <= left_cardinality_max){ // just move a bit ahead the optimum point
                idx_split = 0;
                weight_balance += weights[0].m_weight;
                if(weight_balance < 0){ // if we are including a decreasing section, include as much as possible (i.e. up to the next hammered section)
                    objective = left_cardinality_max;
                    if(weights_length > 1 && weights[1].m_start < objective)
                        objective = weights[1].m_start;
                } else { // if we are including an expanding section, just include as little as possible
                    objective = w_end;
                }

                COUT_DEBUG("corner case, split at interval bound: {" << objective << ", " << idx_split << ", " << (weight_balance) << "}");
                return {objective, idx_split, weight_balance };
            } else { // bad case, we need to split the hammered section
                // decide whether to include the hammered section in the left or right interval
                size_t sect_left_part = left_cardinality_min - w_start;
                size_t sect_right_part = w_end - left_cardinality_min;
                if(sect_left_part < sect_right_part){ // move the hammered section at the right
                    idx_split = -1;
                    weights[0].m_start = objective;
                    weights[0].m_length = sect_right_part;
                } else { // left_part >= sect_right_part
                    idx_split = 0;
                    weights[0].m_length = sect_left_part;
                    weight_balance += weights[0].m_weight;
                }
                COUT_DEBUG("weights[0] adjusted to: " << weights[0]);
                COUT_DEBUG("corner case, split at hammered section: {" << objective << ", " << idx_split << ", " << weight_balance << "}");

                return {objective, idx_split, weight_balance};
            }
        }
    }

    // Find the first index in weights such that it terminates after the min cardinality
    assert(idx_split >= 0 && "It should have been handled by the logic of the corner case above");
    bool stop = false;
    idx_split++;
    while(idx_split < weights_length && !stop){
        auto w_start = weights[idx_split].m_start;
        auto w_end = w_start + weights[idx_split].m_length;
        if(w_start < objective){
            weight_balance += weights[idx_split].m_weight;
            if(w_end < objective){
                idx_split++; // go ahead
            } else {
                stop = true;
            }
        } else { // done
            idx_split--;
            stop = true;
        }
    }

    // Found ?
    if(idx_split < weights_length){
        int64_t w_start = weights[idx_split].m_start;
        int64_t w_end = w_start + weights[idx_split].m_length;
        COUT_DEBUG("idx_split: " << idx_split << ", w_start: " << w_start << ", w_end: " << w_end << ", objective: " << objective << ", overlaps: " << boolalpha << (w_start < objective && w_end > objective));
        if(w_start < objective && w_end > objective){ // the hammered section overlaps with the objective
            if(w_end <= left_cardinality_max){ // can we push the boundary of the interval a bit towards the right ?
                if(weight_balance >= 0){ // include as little as possible
                    objective = w_end;
                } else { // reducing section, include as much as possible
                    objective = left_cardinality_max;
                    if(idx_split +1 < weights_length && weights[idx_split+1].m_start < left_cardinality_max)
                        objective = weights[idx_split +1].m_start;
                }
            } else { // otherwise, we need to split the hammered section in two parts, similarly to what done w/ the corner case
                size_t sect_left_part = left_cardinality_min - w_start;
                size_t sect_right_part = w_end - left_cardinality_min;

                if(sect_left_part < sect_right_part) { // move the hammered section to the right
                    weights[idx_split].m_start = objective;
                    weights[idx_split].m_length = sect_right_part;

                    COUT_DEBUG("split section, to the right: " << weights[idx_split]);

                    weight_balance -= weights[idx_split].m_weight;
                    idx_split--;
                } else { // keep the hammered section in the left interval
                    weights[idx_split].m_length = sect_left_part;
                    COUT_DEBUG("split section, to the left: " << weights[idx_split]);
                }
            }
        }
    } else { // idx_split == weights_length => then all weights are on the left interval
        assert(idx_split == weights_length);
        idx_split--;

        // as we assume our section is decreasing, try to include as many elements as possible
        if(weight_balance < 0){
            objective = left_cardinality_max;
        }
    }

    COUT_DEBUG( (Optimum{ objective, idx_split, weight_balance }) );
    return Optimum { objective, idx_split, weight_balance };

}

Optimum AdaptiveRebalancing::ensure_upper_threshold(size_t left_cardinality_min, size_t left_cardinality_max, Interval* weights, size_t weights_length, int balance, Optimum current){
    int objective = left_cardinality_max;

    // find the first index in `weights' such that weights[index].m_start <= objective
    int idx_split = current.m_weights_index;
    int balance_delta = 0;

    while(idx_split >= 0 && weights[idx_split].m_start >= objective){
        balance_delta += weights[idx_split].m_weight;
        idx_split--;
    }
    COUT_DEBUG("idx_split: " << idx_split << ", W_sz: " << weights_length);

    // found?
    if(idx_split >= 0){
        int64_t w_start = weights[idx_split].m_start;
        int64_t w_end = w_start + weights[idx_split].m_length;
        COUT_DEBUG("w_start: " << w_start << ", w_end: " << w_end << ", overlaps:" << boolalpha  << (w_end > objective));

        assert(w_start < objective && "See the guard of the while loop above");

        if(w_end > objective){ // then this section overlaps

            // can we move the start interval just a bit more to the left?
            if(w_start >= left_cardinality_min){ // include idx_split to the right interval

                balance_delta += weights[idx_split].m_weight;
                idx_split--;
                if(balance_delta >= 0){ // expanding section, include as little as possible
                    objective = w_start;
                } else { // narrowing section, include as much as possible
                    if(idx_split >= 0){
                        objective = max<int>(weights[idx_split].m_start + weights[idx_split].m_length, left_cardinality_min);
                    } else {
                        objective = left_cardinality_min;
                    }
                }
            } else { // we need to split the hammered section
                // note: w_start < objective < w_end
                size_t sect_left_part = objective - w_start;
                size_t sect_right_part = w_end - objective;
                if(sect_left_part < sect_right_part){ // move the hammered section to the right
                    weights[idx_split].m_start = objective;
                    weights[idx_split].m_length = sect_right_part;

                    balance_delta += weights[idx_split].m_weight;
                    idx_split--;
                } else { // keep the hammered section to the left
                    weights[idx_split].m_length = sect_left_part;
                }
            }
        }

        // if the balance is negative (deletes), expand the right section up as much as possible
        else if (balance_delta < 0){
            objective = max<int>(left_cardinality_min, w_end);
        }
    } else {
        // but if we are including more narrowing sectors than expanding ones, extend the section up to the minimum cardinality
        if(balance_delta < 0){
            objective = left_cardinality_min;
        }
    }

    Optimum opt{ objective, idx_split, current.m_weights_balance - balance_delta };
    COUT_DEBUG( opt );
    return opt;
}

Optimum AdaptiveRebalancing::validate_thresholds(size_t part_length, size_t height, Interval* weights, size_t weights_length, int balance, size_t opt_cardinality, Optimum opt){
    // get the bounds
    auto capacity_children = part_length * m_segment_capacity / 2;
    int height_children = height -1; // -1;
    auto bounds = get_density(height_children);
    double rho = bounds.first, theta = bounds.second;

    // compute the minimum & maximum cardinalities
    int64_t size_min = ceil(rho * capacity_children);
    int64_t size_max = theta * capacity_children;

    if(!m_can_fill_segments){ // avoid to yield segments completely full or completely empty
        int64_t num_segments_children = pow(2.0, height_children -1);
        const int64_t min_window_cardinality = num_segments_children;
        const int64_t max_window_cardinality = capacity_children - num_segments_children;
        size_min = max(size_min, min_window_cardinality);
        size_max = min(size_max, max_window_cardinality);
    }

    int64_t fill_min = static_cast<int64_t>(opt_cardinality) - size_max;
    int64_t fill_max = static_cast<int64_t>(opt_cardinality) - size_min;
    int64_t cardinality_min = max(fill_min, size_min);
    int64_t cardinality_max = min(fill_max, size_max);

    COUT_DEBUG("size_min: " << size_min << ", size_max: " << size_max << ", fill_min: " << fill_min << ", fill_max: " << fill_max);
    COUT_DEBUG("min: " << cardinality_min << ", max: " << cardinality_max << ", opt: " << opt.m_cardinality);

    if(opt.m_cardinality < cardinality_min){ // the left partition is too small
        return ensure_lower_threshold(cardinality_min, cardinality_max, weights, weights_length, balance, opt);
    } else if (opt.m_cardinality > cardinality_max){ // the left partition is too big
        return ensure_upper_threshold(cardinality_min, cardinality_max, weights, weights_length, balance, opt);
    } else { // just right
        return opt;
    }
}

void AdaptiveRebalancing::recursion(size_t part_start, size_t part_length, int height, Interval* W, size_t W_sz, int balance, size_t cardinality){
#if defined(DEBUG) /* Header for debug */
    cout << "[AdaptiveRebalancing::recursion] -";
    for(size_t i = 0; i < height; i++) cout << ">";
    cout << " H=" << height << ", start: " << part_start << ", length: " << part_length << ", W_sz: " << W_sz << ", balance: " << balance << ", cardinality: " << cardinality;
    if(W_sz){
        cout << ", weights: [";
        for(size_t i = 0; i < W_sz; i++){
            if(i > 0) cout << ", ";
            cout << W[i];
        }
        cout << "]";
    }
    cout << endl;
#endif


    if(part_length == 1){ // base case
//        m_partitions[part_start] = cardinality;
        emit(cardinality, 1);

        if(W_sz){ move_detector_info(W[0].m_associated_segment, part_start); }

#if defined(DEBUG) // debug only
        if(W_sz){
            cout << "\tSegments associated: [";
            for(size_t i = 0; i < W_sz; i++){
                cout << W[i].m_associated_segment;
                if(i < W_sz -1){ cout << ", "; }
            }
            cout << "]" << endl;
        }
#endif
    } else if (W_sz == 0){ // redistribute evenly
        emit(cardinality, part_length);
    } else if (part_length == 2 && W_sz == 1 && W[0].m_length == m_segment_capacity){
        // special rule, redistribute evenly even though by splitting a full segment
        COUT_DEBUG("Special rule: redistribute evenly!");
        emit(cardinality, part_length);

        // TODO, record hammered segment
        COUT_DEBUG("Segments associated: [" << W[0].m_associated_segment << "]");
    } else { // adaptive strategy
        // step 1: find the optimum split point, regardless of the thresholds
        Optimum opt_left = find_optimum(W, W_sz, balance, cardinality);

        // step 2: ensure the split point is within the lower & upper threshold
        opt_left = validate_thresholds(part_length, height, W, W_sz, balance, cardinality, opt_left);

        // step 3: recursion on the left interval
        size_t w_left_sz = static_cast<size_t>(opt_left.m_weights_index +1);
        recursion(part_start, part_length /2, height -1, W, w_left_sz, opt_left.m_weights_balance, opt_left.m_cardinality);

        // step 4: recursion on the right interval
        Interval* W_right = W + w_left_sz;
        auto W_right_sz = W_sz - w_left_sz;
        int W_offset = opt_left.m_cardinality; // adjust the intervals
        for(size_t i = 0; i < W_right_sz; i++){ W_right[i].m_start -= W_offset; }
        recursion(part_start + part_length /2, part_length /2, height -1, W_right, W_right_sz, balance - opt_left.m_weights_balance, cardinality - opt_left.m_cardinality);

    }
}

// Dump the computed partition, for debug only
void AdaptiveRebalancing::dump(std::ostream& out) const {
    if(m_output_released){
        out << "[AdaptiveRebalancing::dump] Partitions vector already released." << endl;
        return;
    }

    out << "[AdaptiveRebalancing::dump] Partitions vector:\n";
    size_t segment_id = 0; // current segment
    for(auto& partition : m_partitions){
        size_t card_per_segment = partition.m_cardinality / partition.m_segments;
        size_t odd_segments = partition.m_cardinality % partition.m_segments;
        for(size_t j = 0; j < partition.m_segments; j++){
            out << "[" << segment_id << "] cardinality: " << (card_per_segment + (j < odd_segments)) << "\n";
            segment_id++;
        }
    }
}
void AdaptiveRebalancing::dump() const{
    return dump(cout);
}


Optimum::Optimum() : Optimum(0) {}
Optimum::Optimum(int cardinality) : Optimum(cardinality, -1, 0) { }
Optimum::Optimum(int cardinality, int weights_index, int weights_balance) : m_cardinality(cardinality),
        m_weights_index(weights_index), m_weights_balance(weights_balance) { }
std::ostream& operator<<(std::ostream& out, Optimum opt) {
    out << "{OPT cardinality: " << opt.m_cardinality << ", weights index: " << opt.m_weights_index << ", balance: " << opt.m_weights_balance << "}";
    return out;
}

}}} // pma::adaptive::int2
