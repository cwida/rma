/*
 * knobs.cpp
 *
 *  Created on: Mar 8, 2018
 *      Author: dleo@cwi.nl
 */
#include "knobs.hpp"

#include <cassert>

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

Knobs::Knobs(){
    m_rank_threshold = 0.99; // 99% of the values
    m_segment_threshold = 6;
    m_sequence_threshold = 6;
    m_max_sequence_counter = 8;
    m_max_segment_counter = 10;
    m_sampling_rate = 1;
    m_sampling_percentage = 100;
    m_thresholds_switch = 64; // there is some (forgotten...) rationale around this value
}

void Knobs::set_sampling_rate(double value) {
    assert(value >= 0 && value <= 1 && "Invalid value");
    m_sampling_rate = value;
    m_sampling_percentage = value * 100;
}

void Knobs::set_thresholds_switch(int32_t value){
    assert(value >= 1 && "Invalid value");
    m_thresholds_switch = value;
}

ostream& operator<<(ostream& out, const Knobs& settings){
    out << "{APMA/Knobs rank ts: " << settings.get_rank_threshold() << ", " <<
            "segment ts: " << settings.get_segment_threshold() << ", " <<
            "sequence ts: " << settings.get_sequence_threshold() << ", " <<
            "segment max count: " << settings.get_max_segment_counter() << ", " <<
            "sequence max count: " << settings.get_max_sequence_counter() << ", " <<
            "sampling rate: " << settings.get_sampling_rate() << ", " <<
            "[apma_parallel] thresholds switch: " << settings.get_thresholds_switch() << "}";

    return out;
}

}}} // pma::adaptive::int1


