/*
 * rebalancing_profiler.hpp
 *
 *  Created on: 9 Mar 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_ADAPTIVE_INT1_REBALANCING_PROFILER_HPP_
#define PMA_ADAPTIVE_INT1_REBALANCING_PROFILER_HPP_

#include <cinttypes>
#include <vector>

#include "timer.hpp"

namespace pma { namespace adaptive { namespace int1 {

class RebalancingProfiler {
    // compiler barrier
    static inline void barrier(){ __asm__ __volatile__("": : :"memory"); };

    struct ProfileInfo{
        uint64_t m_time_total;  // total time, in microsecs
        uint64_t m_time_search; // search phase, in microsecs
        uint64_t m_time_apma; // apma step, in microsecs
        uint64_t m_time_operation; // spread/resize time, in microsecs
        uint32_t m_length; // window length in case of ::spread or new capacity in case of ::resize
        uint32_t m_previous; // 0 in case of ::spread and old capacity in case of ::resize;
        bool m_on_insert; // true if the rebalance occurred after an insert operation, false otherwise
    };

    // gathered profiles
    mutable std::vector<ProfileInfo> m_profiles;


public:
    class Profiler {
        const RebalancingProfiler* m_base;
        Timer m_timer_total;
        Timer m_timer_search;
        Timer m_timer_apma;
        Timer m_timer_operation; // either spread or resize
        uint32_t m_length = 0; // total number of segments, or new capacity in case of resizing
        uint32_t m_previous = 0; // previous capacity in case of resizing
        const bool m_on_insert;

    public:
        Profiler(RebalancingProfiler* base, bool on_insert);

        ~Profiler();

        void search_start(){ barrier(); m_timer_search.start(); barrier(); }
        void search_stop() { barrier(); m_timer_search.stop(); barrier(); }

        void apma_start(){ barrier(); m_timer_apma.start(); barrier(); }
        void apma_stop() { barrier(); m_timer_apma.stop(); barrier(); }

        void spread_start(size_t window_length){ barrier();  m_length = window_length; m_timer_operation.start(); barrier(); }
        void spread_stop(){ barrier(); m_timer_operation.stop(); barrier(); }

        void resize_start(size_t from, size_t to) { m_length = to; m_previous = from; m_timer_operation.start(); barrier(); }
        void resize_stop() { barrier(); m_timer_operation.stop(); }
    };

    Profiler profiler(bool is_insert) { return Profiler(this, is_insert); }



public:
    struct Statistics {
        uint64_t m_count =0; // number of elements counted
        uint64_t m_sum =0; // sum of the times, in microsecs
        uint64_t m_average =0; // average, in microsecs
        uint64_t m_min =0; // minimum, in microsecs
        uint64_t m_max =0; // maximum, in microsecs
        uint64_t m_stddev =0; // standard deviation, in microsecs
        uint64_t m_median =0; // median, in microsecs
    };

    struct StatisticsRebalances : public Statistics {
        uint64_t m_inserts;  // total number of inserts
        uint64_t m_deletes; // total number of deletes
    };

    struct CompleteStatistics {
        StatisticsRebalances m_cumulative; // total
        Statistics m_search; // search only
        std::vector<std::pair<uint32_t, Statistics>> m_apma; // APMA computations. The first element is the window size. The window 0 is the cumulative statistics.
        std::vector<std::pair<uint32_t, Statistics>> m_spread; // invocations to ::spread
        std::vector<std::pair<uint32_t, Statistics>> m_resize_up; // increase the capacity
        std::vector<std::pair<uint32_t, Statistics>> m_resize_down; // halve the capacity
    };
    CompleteStatistics statistics() const;


    void save_results() const;
};

}}} // pma::adaptive::int1

#endif /* PMA_ADAPTIVE_INT1_REBALANCING_PROFILER_HPP_ */
