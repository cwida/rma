/*
 * weights.cpp
 *
 *  Created on: 7 Mar 2018
 *      Author: Dean De Leo
 */

#include "weights.hpp"

#include <cassert>
#include <iostream>
#include <stdexcept>
#include "detector.hpp"
#include "errorhandling.hpp"
#include "memory_pool.hpp"
#include "packed_memory_array.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[Weights::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

namespace pma { namespace adaptive { namespace int1 {

Weights::Weights(PackedMemoryArray& pma, size_t segment_start, size_t segment_length) :
        m_pma(pma), m_cardinalities(pma.m_storage.m_segment_sizes), m_segment_start(segment_start), m_segment_length(segment_length),
        m_output{ m_pma.memory_pool().allocator<Interval>() }
{
    double threshold = m_pma.knobs().get_rank_threshold();
    if(threshold <= 0 || threshold > 1) throw invalid_argument("[Weights::ctor] Invalid value for the threshold");

#if defined(DEBUG) /* debug only */
    COUT_DEBUG("Cardinalities:");
    for(size_t i = 0; i < m_segment_length; i++){
        cout << "[" << i << "] " << m_cardinalities[m_segment_start + i] << endl;
    }
#endif


    size_t sz = m_pma.detector().modulus() * m_segment_length;
    m_timestamps = m_pma.memory_pool().allocate<int64_t>(sz);

    // step 1: gather all insert & delete keys
    fetch_detector_keys();
#if defined(DEBUG) /* debug only */
    COUT_DEBUG("Detector timestamps: " << m_timestamps_length);
    for(size_t i = 0; i < m_timestamps_length; i++){
        cout << "[" << i << "] " << m_timestamps[i] << endl;
    }
#endif

    // step 2: select the ranked value for inserts & deletions
    int64_t select_threshold;
    if(m_timestamps_length > 0){
        int rank_position = threshold * m_timestamps_length;

        // correct the threshold if it's too high
        int rank_max = max<int>(0, static_cast<int>(m_timestamps_length) - m_pma.detector().modulus());
        rank_position = min(rank_position, rank_max);

        select_threshold = rank(rank_position);
        COUT_DEBUG("Rank: " << rank_position << ", value: " << select_threshold);
    } else {
        select_threshold = std::numeric_limits<int64_t>::max();
    }

    // step 3: prefix sum the cardinalities, to quick discover how many elements precede a given segment
    prefix_sum_cardinalities();

    // step 4: finally identify the intervals that satisfy these weights
    detect_hammered(select_threshold);

    // step 5: remove neutral intervals. This is an edge case, it represents intervals created when two hammered
    // intervals with conflicting signs (insertions/deletions) have been detected
    remove_neutral();

#if defined(DEBUG) /* debug only */
    COUT_DEBUG("Hammered segments: " << m_output.size());
    for(size_t i = 0; i < m_output.size(); i++){
        cout << "[" << i << "] " << m_output[i] << endl;
    }
#endif
}

Weights::~Weights(){
    CachedMemoryPool& memory_pool = m_pma.memory_pool();
    memory_pool.deallocate(m_timestamps); m_timestamps = nullptr;
    memory_pool.deallocate(m_prefix_sum_cardinalities); m_prefix_sum_cardinalities = nullptr;
}

void Weights::prefix_sum_cardinalities(){
    assert(m_prefix_sum_cardinalities == nullptr && "Already initialised");
    m_prefix_sum_cardinalities = m_pma.memory_pool().allocate<int32_t>(m_segment_length);

    m_prefix_sum_cardinalities[0] = m_cardinalities[m_segment_start];
    for(size_t i = 1; i < m_segment_length; i++){
        m_prefix_sum_cardinalities[i] = m_prefix_sum_cardinalities[i -1] + m_cardinalities[m_segment_start +i];
    }
}

size_t Weights::get_cardinality_upto_incl(size_t segment_id) const {
    assert(m_prefix_sum_cardinalities != nullptr && "Array m_prefix_sum_cardinalities not initialised yet. Invoke ::prefix_sum_cardinalities to init it");
    assert(segment_id < m_segment_length);
    return m_prefix_sum_cardinalities[segment_id];
}

size_t Weights::get_cardinality_upto_excl(size_t segment_id) const {
    return segment_id > 0 ? get_cardinality_upto_incl(segment_id -1) : 0;
}

size_t Weights::get_cardinality(size_t segment_id) const  {
    assert(segment_id < m_segment_length && "Index out of bounds");
    return m_cardinalities[m_segment_start + segment_id];
}

int Weights::find_key(size_t segment_id, int64_t key) const noexcept {
    assert(segment_id < m_segment_length && "Index out of bounds");
    return m_pma.find_key(m_segment_start + segment_id, key);
}

void Weights::fetch_detector_keys(){
    Detector& detector = m_pma.detector();
    int64_t* __restrict detector_buffer = detector.buffer() + m_segment_start * detector.sizeof_entry();
    int64_t* __restrict timestamps = m_timestamps;
    for(size_t i = 0; i < m_segment_length; i++){
        int64_t* __restrict section = detector_buffer + detector.sizeof_entry() * i;
//        int16_t* __restrict header = reinterpret_cast<int16_t*>(section);

        for(int h = 0; h < detector.modulus(); h++){
            int64_t value = section[3 + h];
            if(value) timestamps[m_timestamps_length++] = value;
        }
    }
}

int64_t Weights::rank(size_t position){
    return rank(m_timestamps, m_timestamps_length, position);
}

int64_t Weights::rank(int64_t* __restrict A, size_t length, size_t rank){
    assert(length >= 1);
    assert(rank < length);

    while(length > 1){
        auto p = partition(A, length);

#if defined(DEBUG) /** debug only **/
        cout << "Rank: [";
        for(size_t i = 0; i < length; i++){
            cout << i << "=" << A[i];
            if(i < length -1) cout << ", ";
        }
        cout << "] pivot: " << p << ", rank: " << rank << endl;
#endif

        if(p == rank){ // found
            return A[p];
        } else if (rank < p){
            length = p;
        } else { // rank > p
            auto p_1 = p +1;
            length = length - p_1;
            A += p_1;
            rank -= (p_1);

#if defined(DEBUG) /** debug only **/
            cout << "\tAdjustment: [";
            for(size_t i = 0; i < length; i++){
                cout << i << "=" << A[i];
                if(i < length -1) cout << ", ";
            }
            cout << "]\n";
#endif
        }
    }

    // length == 1
    return A[0];
}

size_t Weights::partition(int64_t* __restrict A, size_t length){
    assert(length > 1);

    swap(A[0], A[length /2]);

    int64_t pivot = A[0];
//    COUT_DEBUG("pivot: " << pivot);

    size_t i_lt = 0, i_eq = 1;
    for(size_t i_gt = 1; i_gt < length; i_gt++){
        if (A[i_gt] == pivot){
            swap(A[i_eq], A[i_gt]); i_eq++;
        } else if (A[i_gt] < pivot){
            swap(A[i_lt], A[i_gt]); i_lt++;
            swap(A[i_eq], A[i_gt]); i_eq++;
        }
    }

    return i_lt;
}

void Weights::detect_hammered(int64_t select_threshold){
    Detector& detector = m_pma.detector();

    int apma_segment_threshold = m_pma.knobs().get_segment_threshold();
    int apma_sequence_threshold = m_pma.knobs().get_sequence_threshold();

    int64_t* __restrict detector_buffer = detector.buffer() + m_segment_start * detector.sizeof_entry();
    for(size_t i = 0; i < m_segment_length; i++){
        int64_t* __restrict section = detector_buffer + detector.sizeof_entry() * i;
        int16_t* __restrict header = reinterpret_cast<int16_t*>(section);
        int head = header[0];
        int segment_counter = header[3];
        int weight = 0;

        // FIXME: restore this logic
#if !defined(DEBUG)
        int64_t timestamp_min = section[3 + head];
#else
        // for debug purposes only, find the minimum even when not all timestamps have been seen
        int64_t timestamp_min = 0;
        for(int h = head, stop = detector.modulus(); h < stop && !timestamp_min; h++){
            timestamp_min = section[3 + h];
        }
        for(int h = 0; h < head && !timestamp_min; h++){
            timestamp_min = section[3 + h];
        }
#endif
        COUT_DEBUG("candidate segment: " << i << ", timestamp: " << timestamp_min << ", segment_counter: " << segment_counter);
        if(timestamp_min < select_threshold) continue; // skip this segment

        // candidate for hammering ?
        if(segment_counter > apma_segment_threshold){
            weight = 1;
        } else if(segment_counter < -apma_segment_threshold) {
            weight = -1;
        } else {
            continue; // ignore this segment
        }

        size_t base = get_cardinality_upto_excl(i);
        size_t length = get_cardinality(i);
        COUT_DEBUG("-> hammered segment detected, base: " << base << ", length: " << length << ", weight: " << weight);

        int count_fwd = header[1];
        int count_bwd = header[2];
        auto predecessor = section[1];
        auto successor = section[2];

        if((weight > 0 && count_bwd >= apma_sequence_threshold) || (weight < 0 && count_bwd <= -apma_sequence_threshold)){ // forwards
            int pos_hammered = find_key(i, successor);
            if(pos_hammered != -1){
                if(pos_hammered > 0) pos_hammered--; // as this is the next element
                base += pos_hammered;
                length = 2;
            }
        } else if ((weight > 0 && count_fwd >= apma_sequence_threshold) || (weight < 0 && count_bwd <= -apma_sequence_threshold)){ // backwards
            int pos_hammered = find_key(i, predecessor);
            if(pos_hammered != -1){
                base += pos_hammered;
                length = 2;
            }
        }

        // if we are inserting at the end of the array, the length of the hammered section is actually 1,
        // there no successor elements yet after the hammered point
        if(base + length > get_cardinality_upto_incl(m_segment_length -1))
            length = get_cardinality_upto_incl(m_segment_length -1) - base;


        // in case of deletes, a segment might be empty (...)
        if(length == 0){
            if(base >= get_cardinality_upto_incl(m_segment_length -1)){
                base = get_cardinality_upto_incl(m_segment_length -1) -1;
            }
            length = 1;
        }

        // we have a bit of corner case here, it might happen that a sequenced section with length=2 is followed by a segment section.
        // The two intervals might overlap, because of length =2. In general, let's merge consecutive sections with the same weight
        if(m_output.size() > 0){
            auto& predecessor = m_output.back();
            auto predecessor_wend = predecessor.m_start + predecessor.m_length;

            // do the intervals overlap?
            if(predecessor_wend >= base){
                auto current_wend = base + length;

                // merge the two intervals
                predecessor.m_length = current_wend - predecessor.m_start;

                // This case is interesting, they have different weights. The strategy here is to not report none of the two
                // intervals as `hammered', we simply don't have a clear indication to say which one should be considered hammered
                if(predecessor.m_weight != weight) {
                    m_balance += -(predecessor.m_weight); // roll back the contribution on the global balance
                    predecessor.m_weight = 0; // set a balance of zero, we'll perform a final pass at the end of the algorithm to remove this interval
                    detector.clear(m_segment_start + i); // reset the entry in the detector for this entry, we'll do the same for the predecessor eventually
                }

                continue; // ignore this section
            }
        }

        m_output.push_back(Interval{base, length, weight, i});
        m_balance += weight;
    }
}

void Weights::remove_neutral(){
    Detector& detector = m_pma.detector();

    for(int64_t i = static_cast<int64_t>(m_output.size()) -1; i>=0; i--){
        auto& entry = m_output[i];
        if(entry.m_weight == 0){
            if(entry.m_associated_segment >= 0)
                detector.clear(m_segment_start + entry.m_associated_segment);
            m_output.erase(begin(m_output) + i);
        }
    }
}

VectorOfIntervals Weights::release(){
    if(m_output_released) RAISE_EXCEPTION(Exception, "Vector already released!");
    m_output_released = true;
    return std::move(m_output);
}

int Weights::balance() const noexcept{
    return m_balance;
}

ostream& operator<<(ostream& out, Interval interval){
    out << "{INTERVAL start: " << interval.m_start << ", length: " << interval.m_length << ", weight: " << interval.m_weight;
    if(interval.m_associated_segment >= 0) out << ", associated segment: " << interval.m_associated_segment;
    out << "}";
    return out;
}

}}} // pma::adaptive::int1
