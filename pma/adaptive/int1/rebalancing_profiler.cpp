/*
 * rebalancing_profiler.cpp
 *
 *  Created on: 9 Mar 2018
 *      Author: Dean De Leo
 */

#include "rebalancing_profiler.hpp"

#include <algorithm> // std::sort
#include <cmath>
#include <iostream>
#include "configuration.hpp"
#include "database.hpp"
#include "miscellaneous.hpp"

using namespace std;

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[RebalancingProfiler::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


namespace pma { namespace adaptive { namespace int1 {

RebalancingProfiler::Profiler::Profiler(RebalancingProfiler* base, bool on_insert): m_base(base), m_timer_total(true), m_on_insert(on_insert) {
    COUT_DEBUG("base: " << base << ", on_insert: " << on_insert);
}

RebalancingProfiler::Profiler::~Profiler(){
    m_timer_total.stop();
    m_base->m_profiles.push_back(
            ProfileInfo{ m_timer_total.microseconds<uint64_t>(), m_timer_search.microseconds<uint64_t>(), m_timer_apma.microseconds<uint64_t>(), m_timer_operation.microseconds<uint64_t>(),
        m_length, m_previous, m_on_insert} );
}

RebalancingProfiler::CompleteStatistics RebalancingProfiler::statistics() const {
    CompleteStatistics stats;
    if(m_profiles.empty()) return stats; // corner case
    auto& profiles = m_profiles;

    { // cumulative statistics
        const size_t sz = profiles.size();
        std::sort(begin(profiles), end(profiles), [](const ProfileInfo& p1, const ProfileInfo& p2){
            return p1.m_time_total < p2.m_time_total;
        });

        // median
        if(sz % 2 == 1){
            stats.m_cumulative.m_median = profiles[sz /2].m_time_total;
        } else {
            size_t d1 = sz /2;
            size_t d0 = d1 - 1;
            stats.m_cumulative.m_median = (profiles[d0].m_time_total + profiles[d1].m_time_total) / 2;
        }

        // min & max
        stats.m_cumulative.m_min = profiles[0].m_time_total;
        stats.m_cumulative.m_max = profiles.back().m_time_total;

        // count
        stats.m_cumulative.m_count = sz;

        // average & std. dev.
        uint64_t sum = 0;
        uint64_t sum_sq = 0;
        uint64_t sum_on_insert = 0;
        uint64_t sum_on_delete = 0;
        for(size_t i = 0; i < sz; i++){
            auto& info = profiles[i];
            sum_on_insert += info.m_on_insert == true;
            sum_on_delete += info.m_on_insert == false;
            sum += info.m_time_total;
            sum_sq += (info.m_time_total * info.m_time_total);
        }
        stats.m_cumulative.m_inserts = sum_on_insert;
        stats.m_cumulative.m_deletes = sum_on_delete;
        stats.m_cumulative.m_sum = sum;
        stats.m_cumulative.m_average = sum / sz;
        stats.m_cumulative.m_stddev = (static_cast<double>(sum_sq) / sz) - pow(stats.m_cumulative.m_average, 2.0);
    }

    { // search statistics
        const size_t sz = profiles.size();
        std::sort(begin(profiles), end(profiles), [](const ProfileInfo& p1, const ProfileInfo& p2){
            return p1.m_time_search < p2.m_time_search;
        });

        // median
        if(sz % 2 == 1){
            stats.m_search.m_median = profiles[sz /2].m_time_search;
        } else {
            size_t d1 = sz /2;
            size_t d0 = d1 - 1;
            stats.m_search.m_median = (profiles[d0].m_time_search + profiles[d1].m_time_search) / 2;
        }

        // min & max
        stats.m_search.m_min = profiles[0].m_time_search;
        stats.m_search.m_max = profiles.back().m_time_search;

        // count
        stats.m_search.m_count = sz;

        // average & std. dev.
        uint64_t sum = 0;
        uint64_t sum_sq = 0;
        for(size_t i = 0; i < sz; i++){
            auto& info = profiles[i];
            sum += info.m_time_search;
            sum_sq += (info.m_time_search * info.m_time_search);
        }
        stats.m_search.m_sum = sum;
        stats.m_search.m_average = sum / sz;
        stats.m_search.m_stddev = (static_cast<double>(sum_sq) / sz) - pow(stats.m_search.m_average, 2.0);
    }

    { // cumulative statistics for APMA
        const size_t sz = profiles.size();
        Statistics stats_apma;
        std::sort(begin(profiles), end(profiles), [](const ProfileInfo& p1, const ProfileInfo& p2){
            return p1.m_time_apma < p2.m_time_apma;
        });

        // median
        if(sz % 2 == 1){
            stats_apma.m_median = profiles[sz /2].m_time_apma;
        } else {
            size_t d1 = sz /2;
            size_t d0 = d1 - 1;
            stats_apma.m_median = (profiles[d0].m_time_apma + profiles[d1].m_time_apma) / 2;
        }

        // min & max
        stats_apma.m_min = profiles[0].m_time_apma;
        stats_apma.m_max = profiles.back().m_time_apma;

        // count
        stats_apma.m_count = sz;

        // average & std. dev.
        uint64_t sum = 0;
        uint64_t sum_sq = 0;
        for(size_t i = 0; i < sz; i++){
            auto& info = profiles[i];
            sum += info.m_time_apma;
            sum_sq += (info.m_time_apma * info.m_time_apma);
        }
        stats_apma.m_sum = sum;
        stats_apma.m_average = sum / sz;
        stats_apma.m_stddev = (static_cast<double>(sum_sq) / sz) - pow(stats_apma.m_average, 2.0);

        stats.m_apma.emplace_back(0, stats_apma);
    }

    { // APMA, statistics for each window length
        std::sort(begin(profiles), end(profiles), [](const ProfileInfo& p1, const ProfileInfo& p2){
            return p1.m_length < p2.m_length || (p1.m_length == p2.m_length && p1.m_time_apma < p2.m_time_apma);
        });
        const size_t sz = profiles.size();

        size_t start = 0; // inclusive
        size_t end = 0; // inclusive
        uint64_t sum = 0, sum_sq = 0;
        size_t window_length = 0;

        auto flush = [&stats, &profiles, &start, &end, &window_length, &sum, &sum_sq](){
            if(window_length == 0) return;
            Statistics s;

            // median
            size_t l = end - start +1;
            if(l % 2 ==  1){
                s.m_median = profiles[(start+end) /2].m_time_apma;
            } else {
                size_t d0 = (start + end) /2;
                size_t d1 = d0 +1;
                s.m_median = (profiles[d0].m_time_apma + profiles[d1].m_time_apma) / 2;
            }

            // min & max
            s.m_min = profiles[start].m_time_apma;
            s.m_max = profiles[end].m_time_apma;

            // count
            s.m_count = l;

            // average & std.dev.
            s.m_sum = sum;
            s.m_average = sum / l;
            s.m_stddev = (static_cast<double>(sum_sq) / l) - pow(s.m_average, 2.0);

            stats.m_apma.emplace_back(window_length, s);
        };

        for(size_t i = 0; i < sz; i++){
            auto& info = profiles[i];

            if(info.m_length > window_length){
                end = i -1;
                flush();
                start = i;
                window_length = info.m_length;
                sum = sum_sq = 0;
            }

            sum += info.m_time_apma;
            sum_sq += (info.m_time_apma * info.m_time_apma);
        }

        end = sz -1;
        flush();
    }

    // sort by spread/resize_up/resize_down
    std::sort(begin(profiles), end(profiles), [](const ProfileInfo& p1, const ProfileInfo& p2){
        // first ::spread
        if(p1.m_previous == 0 && p2.m_previous > 0){
            return true;
        } else if (p1.m_previous > 0 && p2.m_previous == 0) {
            return false;
        }  else if (p1.m_previous == 0 && p2.m_previous == 0){
            return p1.m_time_operation < p2.m_time_operation;
        }

        // after capacity increases
        else if (p1.m_length > p1.m_previous && p2.m_length < p1.m_previous){
            return true;
        } else if (p1.m_length < p1.m_previous && p2.m_length > p1.m_previous){
            return false;
        }

        // finally the time
        else {
            return p1.m_time_operation < p2.m_time_operation;
        }

    });
    size_t index_spread_start = 0; // inclusive
    size_t index_spread_end = 0; // exclusive
    size_t index_resize_up_start = 0; // inclusive
    size_t index_resize_up_end = 0;  // exclusive
    size_t index_resize_down_start = 0; // inclusive
    size_t index_resize_down_end = 0; // exclusive
    { // global statistics spread/resize up/resize down
        uint64_t spread_sum = 0;
        uint64_t spread_sum_sq = 0;
        uint64_t resize_up_sum =0;
        uint64_t resize_up_sum_sq = 0;
        uint64_t resize_down_sum = 0;
        uint64_t resize_down_sum_sq = 0;
        const size_t sz = profiles.size();

        size_t i = 0;
        while(i < sz && profiles[i].m_previous == 0){
            spread_sum += profiles[i].m_time_operation;
            spread_sum_sq += (profiles[i].m_time_operation * profiles[i].m_time_operation);
            i++;
        }
        index_spread_end = index_resize_up_start = i;
        while(i < sz && profiles[i].m_previous < profiles[i].m_length){
            resize_up_sum += profiles[i].m_time_operation;
            resize_up_sum_sq += (profiles[i].m_time_operation * profiles[i].m_time_operation);
            i++;
        }
        index_resize_up_end = index_resize_down_start = i;
        while(i < sz){
            assert(profiles[i].m_previous > profiles[i].m_length);
            resize_down_sum += profiles[i].m_time_operation;
            resize_down_sum_sq += (profiles[i].m_time_operation * profiles[i].m_time_operation);
            i++;
        }
        index_resize_down_end = i;
        assert(index_resize_down_end == sz);
        COUT_DEBUG("index_resize_down_start = " << index_resize_down_start << ", index_resize_down_end: " << index_resize_down_end);

        // add the statistics in `where', for the range [start, end).
        auto save_statistics = [&profiles](auto& where, auto start, auto end, auto sum, auto sum_sq){
            Statistics s;

            if(start < end){
                s.m_count = end - start;

                // median
                if(s.m_count % 2 == 1){
                    s.m_median = profiles[(start + end) /2].m_time_operation;
                } else {
                    size_t d1 = (start + end) /2;
                    size_t d0 = d1 - 1;
                    s.m_median = (profiles[d0].m_time_operation + profiles[d1].m_time_operation) / 2;
                }

                // min & max
                s.m_min = profiles[start].m_time_operation;
                s.m_max = profiles[end -1].m_time_operation;

                // average & std. dev.
                s.m_sum = sum;
                s.m_average = sum / s.m_count;
                s.m_stddev = (static_cast<double>(sum_sq) / s.m_count) - pow(s.m_average, 2.0);
            }

            where.emplace_back(0, s);
        };

        save_statistics(stats.m_spread, index_spread_start, index_spread_end, spread_sum, spread_sum_sq);
        save_statistics(stats.m_resize_up, index_resize_up_start, index_resize_up_end, resize_up_sum, resize_up_sum_sq);
        save_statistics(stats.m_resize_down, index_resize_down_start, index_resize_down_end, resize_down_sum, resize_down_sum_sq);
    }


    auto stats_per_operation = [](auto& where /* where to insert, vector */, ProfileInfo* it_start, ProfileInfo* it_end){
        const size_t sz = it_end - it_start;
        std::sort(it_start, it_end, [](const ProfileInfo& p1, const ProfileInfo& p2){
            return p1.m_length < p2.m_length || (p1.m_length == p2.m_length && p1.m_time_operation < p2.m_time_operation);
        });

        size_t start = 0; // inclusive
        size_t end = 0; // exclusive
        uint64_t sum = 0, sum_sq = 0;
        size_t window_length = 0;

        auto flush = [&where, it_start, &start, &end, &window_length, &sum, &sum_sq](){
            if(window_length == 0) return;
            Statistics s;

            // median
            size_t l = end - start;
            if(l % 2 == 1){
                s.m_median = it_start[(start+end) /2].m_time_operation;
            } else {
                size_t d1 = (start + end) /2;
                size_t d0 = d1 -1;
                s.m_median = (it_start[d0].m_time_operation + it_start[d1].m_time_operation) / 2;
            }

            // min & max
            s.m_min = it_start[start].m_time_operation;
            s.m_max = it_start[end -1].m_time_operation;

            // count
            s.m_count = l;

            // average & std.dev.
            s.m_sum = sum;
            s.m_average = sum / l;
            s.m_stddev = (static_cast<double>(sum_sq) / l) - pow(s.m_average, 2.0);

            where.emplace_back(window_length, s);
        };

        size_t i =0 ;
        while(i < sz) {
            auto& info = it_start[i];
            assert(info.m_length >= window_length);

            if(info.m_length > window_length){
                end = i;
                flush();
                start = i;
                window_length = info.m_length;
                sum = sum_sq = 0;
            }

            sum += info.m_time_operation;
            sum_sq += (info.m_time_operation * info.m_time_operation);

            i++;
        }
        end = i;
        flush();
    };

    stats_per_operation(stats.m_spread, profiles.data() + index_spread_start, profiles.data() + index_spread_end);
    stats_per_operation(stats.m_resize_up, profiles.data() + index_resize_up_start, profiles.data() + index_resize_up_end);
    stats_per_operation(stats.m_resize_down, profiles.data() + index_resize_down_start, profiles.data() + index_resize_down_end);

    return stats;
}

void RebalancingProfiler::save_results() const {
    LOG_VERBOSE("Computing the statistics for the rebalances ...");
    CompleteStatistics statistics = this->statistics();
    if(statistics.m_cumulative.m_count == 0) return; // empty!

    if(config().verbose()){
        cout << "[Instrumentation]\n";
        // rebalances
        uint64_t rebalances_count = statistics.m_cumulative.m_count;
        uint64_t rebalances_sum = max<uint64_t>(statistics.m_cumulative.m_sum, 1);
        cout << "--> rebalances (total time): " << rebalances_count << " (on insert: " << statistics.m_cumulative.m_inserts << ", on delete: " << statistics.m_cumulative.m_deletes << "), " <<
                "sum: " << to_string_with_time_suffix(rebalances_sum) << ", " <<
                "average: " << to_string_with_time_suffix(statistics.m_cumulative.m_average) << ", " <<
                "min: " << to_string_with_time_suffix(statistics.m_cumulative.m_min) << ", "  <<
                "max: " << to_string_with_time_suffix(statistics.m_cumulative.m_max) << ", " <<
                "std.dev: " << to_string_with_time_suffix(statistics.m_cumulative.m_stddev) << ", " <<
                "median: " << to_string_with_time_suffix(statistics.m_cumulative.m_median) << "\n";
        cout << "--> rebalances (search only): " << statistics.m_search.m_count << ", " <<
                "sum: " << to_string_with_time_suffix(statistics.m_search.m_sum) << " (" << to_string_2f((100.0 * statistics.m_search.m_sum) / rebalances_sum) << " %), " <<
                "average: " << to_string_with_time_suffix(statistics.m_search.m_average) << ", " <<
                "min: " << to_string_with_time_suffix(statistics.m_search.m_min) << ", "  <<
                "max: " << to_string_with_time_suffix(statistics.m_search.m_max) << ", " <<
                "std.dev: " << to_string_with_time_suffix(statistics.m_search.m_stddev) << ", " <<
                "median: " << to_string_with_time_suffix(statistics.m_search.m_median) << "\n";
        auto print_statistics = [rebalances_count, rebalances_sum](auto name, auto& stats){
            auto count = stats[0].second.m_count;
            if(count == 0) return;
            auto sum = stats[0].second.m_sum;
            cout << "--> " << name << " (total): " << count << " (" << to_string_2f((100.0 * count) / rebalances_count) << " %), "
                 << "sum: " << to_string_with_time_suffix(sum) << " (" << to_string_2f((100.0 * sum) / rebalances_sum) << " %), "
                 << "average: " << to_string_with_time_suffix(stats[0].second.m_average) << ", "
                 << "min: " << to_string_with_time_suffix(stats[0].second.m_min) << ", "
                 << "max: " << to_string_with_time_suffix(stats[0].second.m_max) << ", "
                 << "std. dev: " << to_string_with_time_suffix(stats[0].second.m_stddev) << ", "
                 << "median: " << to_string_with_time_suffix(stats[0].second.m_median) << "\n"
            ;

            for(size_t i = 1, sz = stats.size(); i < sz; i++){
                const auto& s = stats[i].second;
                cout << "--> " << name << " [" << stats[i].first << "]: "
                        << s.m_count << " (" << to_string_2f((100.0 * s.m_count) / count) << " %), "
                        << "sum: " << to_string_with_time_suffix(s.m_sum) << " (" << to_string_2f((100.0 * s.m_sum) / sum) << " %), "
                        << "average: " << to_string_with_time_suffix(s.m_average) << ", "
                        << "min: " << to_string_with_time_suffix(s.m_min) << ", "
                        << "max: " << to_string_with_time_suffix(s.m_max) << ", "
                        << "std. dev: " << to_string_with_time_suffix(s.m_stddev) << ", "
                        << "median: " << to_string_with_time_suffix(s.m_median) << "\n"
                ;
            }
        };

        print_statistics("apma", statistics.m_apma);
        print_statistics("spread", statistics.m_spread);
        print_statistics("resize up", statistics.m_resize_up);
        print_statistics("resize down", statistics.m_resize_down);
    }


    // record statistics
    LOG_VERBOSE("Recording the statistics in the database ...");
    auto save_statistics = [](auto name, auto& statistics, int64_t window_length =0){
        config().db()->add("pma_rebalancing_statistics")
            ("name", name)
            ("window", window_length)
            ("count", statistics.m_count)
            ("sum", statistics.m_sum)
            ("avg", statistics.m_average)
            ("min", statistics.m_min)
            ("max", statistics.m_max)
            ("stddev", statistics.m_stddev)
            ("median", statistics.m_median)
        ;
    };

    auto save_statistics_vector = [&save_statistics](auto name, auto& vect){
        assert(vect.size() > 0 && vect[0].first == 0); // total
        if(vect[0].second.m_count == 0) return;
        save_statistics(name, vect[0].second);

        for(size_t i = 1; i < vect.size(); i++){
            save_statistics(name, vect[i].second, vect[i].first);
        }
    };

    save_statistics("total", statistics.m_cumulative);
    save_statistics("search", statistics.m_search);
    save_statistics_vector("apma", statistics.m_apma);
    save_statistics_vector("spread", statistics.m_spread);
    save_statistics_vector("resize_up", statistics.m_resize_up);
    save_statistics_vector("resize_down", statistics.m_resize_down);
}

}}} // pma::adaptive::int1
