/*
 * knobs.hpp
 *
 *  Created on: Mar 8, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_INT1_KNOBS_HPP_
#define ADAPTIVE_INT1_KNOBS_HPP_

#include <cinttypes>
#include <ostream>

namespace pma { namespace adaptive { namespace int1 {

struct Knobs {
public:
    double m_rank_threshold; // the rank of the element, normalised in [0, 1], to consider as threshold for the minimum timestamp
    uint8_t m_segment_threshold; // the minimum number of counters to consider a segment as candidate as being hammered
    uint8_t m_sequence_threshold; // the minimum value of the counter to infer a contiguous hammered sequence
    uint8_t m_max_sequence_counter; // the maximum value for the counter for the predictor/detector in the bwd/fwd states
    uint8_t m_max_segment_counter; // the maximum value for the counter for for the predictor/detector in the insert/delete states
private:
    double m_sampling_rate; // the sample rate to forward an update to the detector, in [0, 1]
    int32_t m_sampling_percentage; // sample rate in percentage, in [0, 100]
    int32_t m_thresholds_switch; // number of extents after which the ``scan'' (or primary) density thresholds are employed. Only used in apma_parallel.

public:
    Knobs();

    double get_rank_threshold() const;

    uint64_t get_segment_threshold() const;

    uint64_t get_sequence_threshold() const;

    uint64_t get_max_sequence_counter() const;

    uint64_t get_max_segment_counter() const;

    double get_sampling_rate() const;

    void set_sampling_rate(double value);

    int32_t get_sampling_percentage() const;

    uint64_t get_thresholds_switch() const;

    void set_thresholds_switch(int32_t value);
};

std::ostream& operator<<(std::ostream& out, const Knobs& settings);

inline double Knobs::get_rank_threshold() const{ return m_rank_threshold; }
inline uint64_t Knobs::get_segment_threshold() const{ return m_segment_threshold; }
inline uint64_t Knobs::get_sequence_threshold() const{ return m_sequence_threshold; }
inline uint64_t Knobs::get_max_sequence_counter() const{ return m_max_sequence_counter; }
inline uint64_t Knobs::get_max_segment_counter() const{ return m_max_segment_counter; }
inline double Knobs::get_sampling_rate() const { return m_sampling_rate; }
inline int32_t Knobs::get_sampling_percentage() const { return m_sampling_percentage; }
inline uint64_t Knobs::get_thresholds_switch() const { return m_thresholds_switch; }

}}} // pma::adaptive::int1


#endif /* ADAPTIVE_INT1_KNOBS_HPP_ */
