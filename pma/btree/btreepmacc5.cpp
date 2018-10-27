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

#include "btreepmacc5.hpp"

#include <cmath>
#include <cstdlib>
#include <cstring> // memcpy
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "configuration.hpp"
#include "database.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp"

using namespace std;
using namespace pma::btree_pmacc5_details;

namespace pma {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[BTreePMACC5::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Profiling                                                               *
 *                                                                           *
 *****************************************************************************/
#if defined(PROFILING)
namespace btree_pmacc5_details {

Instrumentation::Profiler::~Profiler(){
    m_timer_total.stop();
    m_base->m_profiles.push_back(
            ProfileInfo{ m_timer_total.microseconds<uint64_t>(), m_timer_search.microseconds<uint64_t>(), m_timer_operation.microseconds<uint64_t>(),
        m_length, m_previous, m_on_insert} );
}

Instrumentation::CompleteStatistics Instrumentation::statistics() const {
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

    // sort by spread/resize_up/resize_down
    std::sort(begin(profiles), end(profiles), [](const ProfileInfo& p1, const ProfileInfo& p2){
        // first ::spread
        if(p1.m_previous == 0 && p2.m_previous > 0){
            return true;
        } else if (p1.m_previous > 0 && p2.m_previous == 0) {
            return false;
        }  else if (p1.m_previous == 0 && p2.m_previous == 0){
            return p1.m_time_operation < p2.m_time_operation;
        } else {

            // in case of resizes, m_length is the new capacity, and m_previous is the old capacity
            bool p1_resize_up = p1.m_length > p1.m_previous;
            bool p2_resize_up = p2.m_length > p2.m_previous;
            if (p1_resize_up && !p2_resize_up){
                return true;
            } else if (p2_resize_up && !p1_resize_up){
                return false;
            }

            // finally the time
            else {
                assert(p1_resize_up == p2_resize_up && "These should both resizes");
                return p1.m_time_operation < p2.m_time_operation;
            }

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
} // btree_pmacc5_details

void BTreePMACC5::record_rebalancing_statistics() const {
    LOG_VERBOSE("Computing the statistics for the rebalances ...");
    Instrumentation::CompleteStatistics statistics = m_instrumentation.statistics();
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

        print_statistics("spread", statistics.m_spread);
        print_statistics("resize up", statistics.m_resize_up);
        print_statistics("resize down", statistics.m_resize_down);
    }


    // if we are in `testing mode' there won't be any database instance
    if(config().db() != nullptr){

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
        save_statistics_vector("spread", statistics.m_spread);
        save_statistics_vector("resize_up", statistics.m_resize_up);
        save_statistics_vector("resize_down", statistics.m_resize_down);

    }
}
#endif

/*****************************************************************************
 *                                                                           *
 *   Initialization                                                          *
 *                                                                           *
 *****************************************************************************/

BTreePMACC5::BTreePMACC5() : BTreePMACC5(/* B = */ 64) { }
BTreePMACC5::BTreePMACC5(size_t btree_block_size) : BTreePMACC5(btree_block_size, btree_block_size) { }
BTreePMACC5::BTreePMACC5(size_t btree_block_size, size_t pma_segment_size) :
       m_index(btree_block_size),
       m_storage(pma_segment_size) {
}

BTreePMACC5::~BTreePMACC5() {
    if(m_segment_statistics) record_segment_statistics();

    free(m_storage.m_keys); m_storage.m_keys = nullptr;
    free(m_storage.m_values); m_storage.m_values = nullptr;
    free(m_storage.m_segment_sizes); m_storage.m_segment_sizes = nullptr;

#if defined(PROFILING)
    record_rebalancing_statistics();
#endif
}

PMA::PMA(size_t segment_size) : m_segment_capacity( hyperceil(segment_size ) ){
    if(hyperceil(segment_size ) > numeric_limits<uint16_t>::max()) throw std::invalid_argument("segment size too big, maximum is " + std::to_string( numeric_limits<uint16_t>::max() ));
    if(m_segment_capacity < 8) throw std::invalid_argument("segment size too small, minimum is 8");

    m_capacity = m_segment_capacity;
    m_number_segments = 1;
    m_height = 1;
    m_cardinality = 0;

    // memory allocations
    alloc_workspace(1, &m_keys, &m_values, &m_segment_sizes);
}

void PMA::alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes){
    // reset the ptrs
    *keys = nullptr;
    *values = nullptr;
    *sizes = nullptr;

    int rc(0);
    rc = posix_memalign((void**) keys, /* alignment */ 64,  /* size */ num_segments * m_segment_capacity * sizeof(m_keys[0]));
    if(rc != 0) {
        RAISE_EXCEPTION(Exception, "[PMA::PMA] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << m_segment_capacity * sizeof(m_keys[0]));
    }
    rc = posix_memalign((void**) values, /* alignment */ 64,  /* size */ num_segments * m_segment_capacity * sizeof(m_values[0]));
    if(rc != 0) {
        free(*keys); *keys = nullptr;
        RAISE_EXCEPTION(Exception, "[PMA::PMA] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << m_segment_capacity * sizeof(m_values[0]));
    }

    rc = posix_memalign((void**) sizes, /* alignment */ 64,  /* size */ num_segments * sizeof(m_segment_sizes[0]));
    if(rc != 0) {
        free(*keys); *keys = nullptr;
        free(*values); *values = nullptr;
        RAISE_EXCEPTION(Exception, "[PMA::PMA] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << m_segment_capacity * sizeof(m_segment_sizes[0]));
    }
}

size_t BTreePMACC5::size() const {
    return m_storage.m_cardinality;
}

bool BTreePMACC5::empty() const noexcept {
    return m_storage.m_cardinality == 0;
}

pair<double, double> BTreePMACC5::thresholds(int height) {
    return thresholds(height, m_storage.m_height);
}

pair<double, double> BTreePMACC5::thresholds(int node_height, int tree_height) {
    return m_density_bounds.thresholds(tree_height, node_height);
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/

void BTreePMACC5::insert(int64_t key, int64_t value){
    if(UNLIKELY( empty() )){
        insert_empty(key, value);
    } else {
        size_t segment = m_index.find(key);
        insert_common(segment, key, value);
    }

#if defined(DEBUG)
    dump();
#endif
}


void BTreePMACC5::insert_empty(int64_t key, int64_t value){
    assert(empty());
    assert(m_storage.m_capacity > 0 && "The storage does not have any capacity?");

    m_index.set_separator_key(0, key);
    m_storage.m_segment_sizes[0] = 1;
    size_t pos = m_storage.m_segment_capacity -1;
    m_storage.m_keys[pos] = key;
    m_storage.m_values[pos] = value;
    m_storage.m_cardinality = 1;
}

void BTreePMACC5::insert_common(size_t segment_id, int64_t key, int64_t value){
    assert(!empty() && "Wrong method: use ::insert_empty");
    assert(segment_id < m_storage.m_capacity && "Overflow: attempting to access an invalid segment in the PMA");

    COUT_DEBUG("segment_id: " << segment_id << ", element: <" << key << ", " << value << ">");

    // is this bucket full ?
    auto bucket_cardinality = m_storage.m_segment_sizes[segment_id];
    if(bucket_cardinality == m_storage.m_segment_capacity){
        rebalance(segment_id, &key, &value);
    } else { // find a spot where to insert this element
        bool minimum_updated = storage_insert_unsafe(segment_id, key, value);

        // have we just updated the minimum ?
        if (minimum_updated) m_index.set_separator_key(segment_id, key);
    }
}

bool BTreePMACC5::storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value){
    assert(m_storage.m_segment_sizes[segment_id] < m_storage.m_segment_capacity && "This segment is full!");

    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    bool minimum = false; // the inserted key is the new minimum ?
    size_t sz = m_storage.m_segment_sizes[segment_id];

    if(segment_id % 2 == 0){ // for even segment ids (0, 2, ...), insert at the end of the segment
        size_t stop = m_storage.m_segment_capacity -1;
        size_t start = m_storage.m_segment_capacity - sz -1;
        size_t i = start;

        while(i < stop && keys[i+1] < key){
            keys[i] = keys[i+1];
            i++;
        }

        COUT_DEBUG("(even) segment_id: " << segment_id << ", start: " << start << ", stop: " << stop << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = start; j < i; j++){
            values[j] = values[j+1];
        }
        values[i] = value;

        minimum = (i == start);
    } else { // for odd segment ids (1, 3, ...), insert at the front of the segment
        size_t i = sz;
        while(i > 0 && keys[i-1] > key){
            keys[i] = keys[i-1];
            i--;
        }

        COUT_DEBUG("(odd) segment_id: " << segment_id << ", key: " << key << ", value: " << value << ", position: " << i);
        keys[i] = key;

        for(size_t j = sz; j > i; j--){
            values[j] = values[j-1];
        }
        values[i] = value;

        minimum = (i == 0);
    }

    // update the cardinality
    m_storage.m_segment_sizes[segment_id]++;
    m_storage.m_cardinality += 1;

    return minimum;
}

void BTreePMACC5::spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value){
    size_t i = 0;
    while(i < num_elements && keys_from[i] < new_key){
        keys_to[i] = keys_from[i];
        values_to[i] = values_from[i];
        i++;
    }
    keys_to[i] = new_key;
    values_to[i] = new_value;

    memcpy(keys_to + i + 1, keys_from + i, (num_elements -i) * sizeof(keys_to[0]));
    memcpy(values_to + i + 1, values_from + i, (num_elements -i) * sizeof(values_to[0]));

//#if defined(DEBUG)
//    for(size_t i =0; i < num_elements +1; i++){
//        cout << "merge [" << i << "] <" << keys_to[i] << ", " << values_to[i] << ">\n";
//    }
//#endif

    m_storage.m_cardinality++;
}

/*****************************************************************************
 *                                                                           *
 *   Rebalance                                                               *
 *                                                                           *
 *****************************************************************************/
void BTreePMACC5::rebalance(size_t segment_id, int64_t* key, int64_t* value){
    assert(((key && value) || (!key && !value)) && "Either both key & value are specified (insert) or none of them is (delete)");
    const bool is_insert = key != nullptr;
    auto profiler = m_instrumentation.profiler(is_insert);
    profiler.search_start();

    COUT_DEBUG("segment_id: " << segment_id);
    size_t num_elements = is_insert ? m_storage.m_segment_capacity +1 : m_storage.m_segment_sizes[segment_id];
    // these inits are only valid for the edge case that the calibrator tree has height 1, i.e. the data structure contains only one segment
    double rho = 0.0, theta = 1.0, density = static_cast<double>(num_elements)/m_storage.m_segment_capacity;
    size_t height = 1;
    COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta << ", num_elements: " << num_elements);

    int window_length = 1;
    int window_id = segment_id;
    int window_start = segment_id, window_end = segment_id;

    if(m_storage.m_height > 1){
        // find the bounds of this window
        int index_left = segment_id -1;
        int index_right = segment_id +1;

        do {
            height++;
            window_length *= 2;
            window_id /= 2;
            window_start = window_id * window_length;
            window_end = window_start + window_length;
            auto density_bounds = thresholds(height);
            rho = density_bounds.first;
            theta = density_bounds.second;

            // find the number of elements in the interval
            while(index_left >= window_start){
                num_elements += m_storage.m_segment_sizes[index_left];
                index_left--;
            }
            while(index_right < window_end){
                num_elements += m_storage.m_segment_sizes[index_right];
                index_right++;
            }

            COUT_DEBUG("num_elements: " << num_elements << ", window_start: " << window_start << ",  window_length: " << window_length << ",  segment_capacity: " << m_storage.m_segment_capacity);
            density = ((double) num_elements) / (window_length * m_storage.m_segment_capacity);

            COUT_DEBUG("height: " << height << ", density: " << density << ", rho: " << rho << ", theta: " << theta);
        } while (
                ((is_insert && density > theta) || (!is_insert && density < rho))
                && height < m_storage.m_height);
    }

    profiler.search_stop();

    if((is_insert &&  density <= theta) || (!is_insert && density >= rho)){
        spread_insert spread_insert, *spread_insert_ptr = nullptr;
        if(is_insert){ spread_insert = { *key, *value, segment_id }; spread_insert_ptr = &spread_insert; }

        // if this is due to an insertion, num_elements is already +1 for the key being inserted
        COUT_DEBUG("--SPREAD--");
        profiler.spread_start(window_length);
        spread(num_elements, window_start, window_length, spread_insert_ptr);
        profiler.spread_stop();
    } else {
        COUT_DEBUG("--RESIZE--");

#if defined(PROFILING)
        if(is_insert)
            profiler.resize_start(m_storage.m_capacity, m_storage.m_capacity *2);
        else
            profiler.resize_start(m_storage.m_capacity, m_storage.m_capacity /2);
#endif

        resize(key, value);

#if defined(PROFILING)
        profiler.resize_stop();
#endif
    }
}

void BTreePMACC5::resize(int64_t* new_key, int64_t* new_value) {
    const bool is_insert = new_key != nullptr;
    size_t capacity = is_insert ?  m_storage.m_capacity * 2 : m_storage.m_capacity / 2; // new capacity
    size_t num_segments = capacity / m_storage.m_segment_capacity;
    size_t elements_per_segment = m_storage.m_cardinality / num_segments;
    size_t odd_segments = m_storage.m_cardinality % num_segments;
    COUT_DEBUG(m_storage.m_capacity << " --> " << capacity << ", num_segments: " << num_segments);

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_sizes) ixSizes;
    m_storage.alloc_workspace(num_segments, &ixKeys, &ixValues, &ixSizes);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_sizes);
    auto xFreePtr = [](void* ptr){ free(ptr); };
    unique_ptr<int64_t, decltype(xFreePtr)> ixKeys_ptr { ixKeys, xFreePtr };
    unique_ptr<int64_t, decltype(xFreePtr)> ixValues_ptr{ ixValues, xFreePtr };
    unique_ptr<remove_pointer_t<decltype(m_storage.m_segment_sizes)>, decltype(xFreePtr)> ixSizes_ptr{ ixSizes, xFreePtr };
    int64_t* __restrict xKeys = m_storage.m_keys;
    int64_t* __restrict xValues = m_storage.m_values;
    decltype(m_storage.m_segment_sizes) __restrict xSizes = m_storage.m_segment_sizes;

    m_index.rebuild(num_segments);

    // fetch the first non-empty input segment
    size_t input_segment_id = 0;
    size_t input_size = ixSizes[0];
    int64_t* input_keys = ixKeys + m_storage.m_segment_capacity;
    int64_t* input_values = ixValues + m_storage.m_segment_capacity;
    bool input_segment_odd = false; // consider '0' as even
    if(input_size == 0){ // corner case, the first segment is empty!
        assert(!is_insert && "Otherwise we shouldn't see empty segments");
        input_segment_id = 1;
        input_segment_odd = true; // segment '1' is odd
        input_size = ixSizes[1];
    } else { // stick to the first segment, even!
        input_keys -= input_size;
        input_values -= input_size;
    }

    // start copying the elements
    bool output_segment_odd = false; // consider '0' as even
    for(size_t j = 0; j < num_segments; j++){
        // copy `elements_per_segment' elements at the start
        size_t elements_to_copy = elements_per_segment;
        if ( j < odd_segments ) elements_to_copy++;
        COUT_DEBUG("j: " << j << ", elements_to_copy: " << elements_to_copy);

        size_t output_offset = output_segment_odd ? 0 : m_storage.m_segment_capacity - elements_to_copy;
        size_t output_canonical_index = j * m_storage.m_segment_capacity;
        int64_t* output_keys = xKeys + output_canonical_index + output_offset;
        int64_t* output_values = xValues + output_canonical_index + output_offset;
        xSizes[j] = elements_to_copy;
        m_index.set_separator_key(j, input_keys[0]);

        do {
            assert(elements_to_copy <= m_storage.m_segment_capacity && "Overflow");

            size_t cpy1 = min(elements_to_copy, input_size);
            memcpy(output_keys, input_keys, cpy1 * sizeof(m_storage.m_keys[0]));
            output_keys += cpy1; input_keys += cpy1;
            memcpy(output_values, input_values, cpy1 * sizeof(m_storage.m_values[0]));
            output_values += cpy1; input_values += cpy1;
            input_size -= cpy1;
            COUT_DEBUG("cpy1: " << cpy1 << ", elements_to_copy: " << elements_to_copy - cpy1 << ", input_size: " << input_size);

            if(input_size == 0){ // move to the next input segment
                input_segment_id++;
                input_segment_odd = !input_segment_odd;

                if(input_segment_id < m_storage.m_number_segments){ // avoid overflows
                    input_size = ixSizes[input_segment_id];

                    // in case of ::remove(), we might find an empty segment, skip it!
                    if(input_size == 0){
                        assert(!is_insert && "Otherwise we shouldn't see empty segments");
                        input_segment_id++;
                        input_segment_odd = !input_segment_odd; // flip again
                        if(input_segment_id < m_storage.m_number_segments){
                            input_size = ixSizes[input_segment_id];
                            assert(input_size > 0 && "Only a single empty segment should exist...");
                        }
                    }

                    size_t offset = input_segment_odd ? 0 : m_storage.m_segment_capacity - input_size;
                    size_t input_canonical_index = input_segment_id * m_storage.m_segment_capacity;
                    input_keys = ixKeys + input_canonical_index + offset;
                    input_values = ixValues + input_canonical_index + offset;
                }
                assert(input_segment_id <= (m_storage.m_number_segments +1) && "Infinite loop");
            }

            elements_to_copy -= cpy1;
        } while(elements_to_copy > 0);

        // should we insert a new element in this bucket
        if(new_key && *new_key < output_keys[-1]){
            auto min = storage_insert_unsafe(j, *new_key, *new_value);
            if(min) m_index.set_separator_key(j, *new_key); // update the minimum in the B+ tree
            new_key = new_value = nullptr;
        }

        output_segment_odd = !output_segment_odd; // flip
    }

    // if the element hasn't been inserted yet, it means it has to be placed in the last segment
    if(new_key){
        auto min = storage_insert_unsafe(num_segments -1, *new_key, *new_value);
        if(min) m_index.set_separator_key(num_segments -1, *new_key); // update the minimum in the B+ tree
        new_key = new_value = nullptr;
    }

    // update the PMA properties
    m_storage.m_capacity = capacity;
    m_storage.m_number_segments = num_segments;
    m_storage.m_height = log2(num_segments) +1;

    // side effect: regenerate the thresholds
    thresholds(m_storage.m_height, m_storage.m_height);
}

void BTreePMACC5::spread(size_t cardinality, size_t segment_start, size_t num_segments, spread_insert* spread_insertion){
    int64_t insert_segment_id = spread_insertion != nullptr ? static_cast<int64_t>(spread_insertion->m_segment_id) - segment_start : -1;
    COUT_DEBUG("size: " << cardinality << ", start: " << segment_start << ", length: " << num_segments << ", insertion segment: " << insert_segment_id);
    assert(segment_start % 2 == 0 && "Expected to start from an even segment");
    assert(num_segments % 2 == 0 && "Expected an even number of segments");

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    segment_size_t* __restrict sizes = m_storage.m_segment_sizes + segment_start;
    int64_t* __restrict output_keys = m_storage.m_keys + segment_start * m_storage.m_segment_capacity;
    int64_t* __restrict output_values = m_storage.m_values + segment_start * m_storage.m_segment_capacity;

    // input chunk 2 (extra space)
    const size_t input_chunk2_capacity = static_cast<size_t>(m_storage.m_segment_capacity) *4 +1;
    size_t input_chunk2_size = 0;
    auto& memory_pool = m_memory_pool;
    auto memory_pool_deleter = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_chunk2_keys_ptr { m_memory_pool.allocate<int64_t>(input_chunk2_capacity), memory_pool_deleter };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_chunk2_values_ptr {  m_memory_pool.allocate<int64_t>(input_chunk2_capacity), memory_pool_deleter };
    int64_t* __restrict input_chunk2_keys = input_chunk2_keys_ptr.get();
    int64_t* __restrict input_chunk2_values = input_chunk2_values_ptr.get();

    // input chunk1 (it overlaps the current window)
    int64_t* __restrict input_chunk1_keys = nullptr;
    int64_t* __restrict input_chunk1_values = nullptr;
    size_t input_chunk1_size = 0;

    { // 1) first, compact all elements towards the end
        int64_t output_segment_id = num_segments -2;
        int64_t output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
        int64_t output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];

        // copy the last four segments into input_chunk2_capacity
        int input_chunk2_segments_copied = 0;
        size_t input_chunk2_space_left = input_chunk2_capacity;
        while(output_segment_id >= 0 && input_chunk2_segments_copied < 4){
            size_t elements2copy = output_end - output_start;
            COUT_DEBUG("input_chunk2_segments_copied: " << input_chunk2_segments_copied << ", input_chunk2_space_left: " << input_chunk2_space_left << ", output_segment_id: " << output_segment_id << ", elements2copy: " << elements2copy);
            if(insert_segment_id == output_segment_id || insert_segment_id == output_segment_id +1){
                spread_insert_unsafe(output_keys + output_start, output_values + output_start,
                        input_chunk2_keys + input_chunk2_space_left - elements2copy -1, input_chunk2_values + input_chunk2_space_left - elements2copy -1,
                        elements2copy, spread_insertion->m_key, spread_insertion->m_value);
                input_chunk2_space_left--;
            } else {
                memcpy(input_chunk2_keys + input_chunk2_space_left - elements2copy, output_keys + output_start, elements2copy * sizeof(input_chunk2_keys[0]));
                memcpy(input_chunk2_values + input_chunk2_space_left - elements2copy, output_values + output_start, elements2copy * sizeof(input_chunk2_values[0]));
            }
            input_chunk2_space_left -= elements2copy;

            // fetch the next chunk
            output_segment_id -= 2;
            if(output_segment_id >= 0){
                output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
                output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
            }

            input_chunk2_segments_copied += 2;
        }

        // readjust the pointers for input_chunk2
        input_chunk2_keys += input_chunk2_space_left;
        input_chunk2_values += input_chunk2_space_left;
        input_chunk2_size = input_chunk2_capacity - input_chunk2_space_left;

        // move the remaining elements towards the end of the array
        int64_t input_chunk1_current = num_segments * m_storage.m_segment_capacity;
        while(output_segment_id >= 0){
            size_t elements2copy = output_end - output_start;
            if(insert_segment_id == output_segment_id || insert_segment_id == output_segment_id +1){
                spread_insert_unsafe(output_keys + output_start, output_values + output_start,
                        output_keys + input_chunk1_current - elements2copy -1, output_values + input_chunk1_current - elements2copy -1,
                        elements2copy, spread_insertion->m_key, spread_insertion->m_value);
                input_chunk1_current--;
            } else {
                memcpy(output_keys + input_chunk1_current - elements2copy, output_keys + output_start, elements2copy * sizeof(output_keys[0]));
                memcpy(output_values + input_chunk1_current - elements2copy, output_values + output_start, elements2copy * sizeof(output_values[0]));
            }
            input_chunk1_current -= elements2copy;

            // fetch the next chunk
            output_segment_id -= 2;
            if(output_segment_id >= 0){
                output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
                output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
            }
        }

        // readjust the pointers for input_chunk1
        input_chunk1_size = num_segments * m_storage.m_segment_capacity - input_chunk1_current;
        input_chunk1_keys = output_keys + input_chunk1_current;
        input_chunk1_values = output_values + input_chunk1_current;
    }

    // debug only
#if defined(DEBUG)
    size_t k = 0;
    for(size_t i = 0; i < input_chunk1_size; i++){
        cout << "Chunk 1 [" << k++ << "] <" << input_chunk1_keys[i] << ", " << input_chunk1_values[i] << ">\n";
    }
    for(size_t i = 0; i < input_chunk2_size; i++){
        cout << "Chunk 2 [" << k++ << "] <" << input_chunk2_keys[i] << ", " << input_chunk2_values[i] << ">\n";
    }
    std::flush(cout);
#endif

    // 2) set the expected size of each segment
    const size_t elements_per_segment = cardinality / num_segments;
    const size_t num_odd_segments = cardinality % num_segments;
    for(size_t i = 0; i < num_segments; i++){
        sizes[i] = elements_per_segment + (i < num_odd_segments);
    }

    // 3) initialise the input chunk
    int64_t* __restrict input_keys;
    int64_t* __restrict input_values;
    size_t input_current = 0;
    size_t input_size;
    if(input_chunk1_size > 0){
        input_keys = input_chunk1_keys;
        input_values = input_chunk1_values;
        input_size = input_chunk1_size;
    } else {
        input_keys = input_chunk2_keys;
        input_values = input_chunk2_values;
        input_size = input_chunk2_size;
    }

    // 4) copy from the input chunks
    COUT_DEBUG("cardinality: " << cardinality << ", chunk1 size: " << input_chunk1_size << ", chunk2 size: " << input_chunk2_size);
    for(size_t i = 0; i < num_segments; i+=2){
        const size_t output_start = (i +1) * m_storage.m_segment_capacity - sizes[i];
        const size_t output_end = output_start + sizes[i] + sizes[i+1];
        size_t output_current = output_start;

        COUT_DEBUG("segments: [" << i << ", " << i+1 << "], required elements: " << output_end - output_start);

        while(output_current < output_end){
            size_t elements2copy = min(output_end - output_current, input_size - input_current);
            COUT_DEBUG("elements2copy: " << elements2copy << " output_start: " << output_start << ", output_end: " << output_end << ", output_current: " << output_current);
            memcpy(output_keys + output_current, input_keys + input_current, elements2copy * sizeof(output_keys[0]));
            memcpy(output_values + output_current, input_values + input_current, elements2copy * sizeof(output_values[0]));
            output_current += elements2copy;
            input_current += elements2copy;
            // switch to the second chunk
            if(input_current == input_size && input_keys == input_chunk1_keys){
                input_keys = input_chunk2_keys;
                input_values = input_chunk2_values;
                input_size = input_chunk2_size;
                input_current = 0;
            }
        }

        // update the separator keys
        m_index.set_separator_key(segment_start + i, output_keys[output_start]);
        m_index.set_separator_key(segment_start + i + 1, output_keys[output_start + sizes[i]]);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t BTreePMACC5::remove(int64_t key){
    if(empty()) return -1;

    auto segment_id = m_index.find(key);
    COUT_DEBUG("key: " << key << ", bucket: " << segment_id);
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];
    assert(sz > 0 && "Empty segment!");

    int64_t value = -1;

    if (segment_id % 2 == 0) { // even
        size_t imin = m_storage.m_segment_capacity - sz, i;
        for(i = imin; i < m_storage.m_segment_capacity; i++){ if(keys[i] == key) break; }
        if(i < m_storage.m_segment_capacity){ // found ?
            value = values[i];
            // shift the rest of the elements by 1
            for(size_t j = i; j > imin; j--){
                keys[j] = keys[j -1];
                values[j] = values[j-1];
            }

            sz--;
            m_storage.m_segment_sizes[segment_id] = sz;
            m_storage.m_cardinality--;

            if(i == imin){ // update the pivot
                if(m_storage.m_cardinality == 0){ // global minimum
                    m_index.set_separator_key(0, numeric_limits<int64_t>::min());
                } else {
                    m_index.set_separator_key(segment_id, keys[imin +1]);
                }
            }
        } // end if (found)
    } else { // odd
        // find the key in the segment
        size_t i = 0;
        for( ; i < sz; i++){ if(keys[i] == key) break; }
        if(i < sz){ // found?
            value = values[i];
            // shift the rest of the elements by 1
            for(size_t j = i; j < sz - 1; j++){
                keys[j] = keys[j+1];
                values[j] = values[j+1];
            }

            sz--;
            m_storage.m_segment_sizes[segment_id] = sz;
            m_storage.m_cardinality--;

            // update the minimum
            if(i == 0 && sz > 0){ // sz > 0 => otherwise we are going to rebalance this segment anyway
                m_index.set_separator_key(segment_id, keys[0]);
            }
        } // end if (found)
    } // end if (odd segment)

    // shall we rebalance ?
    if(value != -1 && m_storage.m_number_segments > 1){
        const size_t minimum_size = max<size_t>(thresholds(1).first * m_storage.m_segment_capacity, 1); // at least one element per segment
        if(sz < minimum_size){ rebalance(segment_id, nullptr, nullptr); }
    }

#if defined(DEBUG)
    dump();
#endif

    return value;
}


/*****************************************************************************
 *                                                                           *
 *   Search                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t BTreePMACC5::find(int64_t key) const {
    if(empty()) return -1;

    auto segment_id = m_index.find(key);
    COUT_DEBUG("key: " << key << ", bucket: " << segment_id);
    int64_t* __restrict keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    size_t sz = m_storage.m_segment_sizes[segment_id];

    size_t start, stop;

    if(segment_id % 2 == 0){ // even
        stop = m_storage.m_segment_capacity;
        start = stop - sz;
    } else { // odd
        start = 0;
        stop = sz;
    }

    for(size_t i = start; i < stop; i++){
        if(keys[i] == key){
            return *(m_storage.m_values + segment_id * m_storage.m_segment_capacity + i);
        }
    }

    return -1;
}


/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/

namespace btree_pmacc5_details {

Iterator::Iterator(const PMA& storage) : m_pma(storage) { } // empty iterator

Iterator::Iterator(const PMA& storage, size_t segment_start, size_t segment_end, int64_t key_min, int64_t key_max) : m_pma(storage){
    if(segment_start > segment_end) throw invalid_argument("segment_start > segment_end");
    COUT_DEBUG("segment_start: " << segment_start << ", segment_end: " << segment_end << ", key_min: " << key_min << ", key_max: " << key_max);
    if(segment_end >= storage.m_number_segments) return;
    int64_t* __restrict keys = storage.m_keys;

    bool notfound = true;
    ssize_t segment_id = segment_start;
    bool segment_even = segment_id % 2 == 0;
    ssize_t start, stop = -1, offset = -1;

    while(notfound && segment_id < storage.m_number_segments){
        if(segment_even){
            stop = (segment_id +1) * storage.m_segment_capacity;
            start = stop - storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, even segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
        } else { // odd
            start = segment_id * storage.m_segment_capacity;
            stop = start + storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, odd segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
        }
        offset = start;

        while(offset < stop && keys[offset] < key_min) {
            COUT_DEBUG("lower interval, offset: " << offset << ", key: " << keys[offset] << ", key_min: " << key_min);
            offset++;

        }

        notfound = (offset == stop);
        if(notfound){
            segment_id++;
            segment_even = !segment_even; // flip
        }
    }

    m_offset = offset;
    m_next_segment = segment_id +1;
    m_stop = stop;
    if(segment_even && m_next_segment < storage.m_number_segments){
        m_stop = m_next_segment * storage.m_segment_capacity + storage.m_segment_sizes[m_next_segment];
        m_next_segment++;
    }

    if(notfound || keys[m_offset] > key_max){
        COUT_DEBUG("#1 - no qualifying interval for min");
        m_index_max = m_stop = 0;
    } else {
        // find the last qualifying index
        assert(segment_end < storage.m_number_segments);
        auto interval_start_segment = segment_id;
        segment_id = segment_end;
        segment_even = segment_id % 2 == 0;
        notfound = true;

        while(notfound && segment_id >= interval_start_segment){
            if(segment_even){
                start = (segment_id +1) * storage.m_segment_capacity -1;
                stop = start - storage.m_segment_sizes[segment_id];
            } else { // odd
                stop = segment_id * storage.m_segment_capacity;
                start = stop + storage.m_segment_sizes[segment_id] -1;
            }
            COUT_DEBUG("upper interval, " << (segment_even ? "even":"odd") << " segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
            offset = start;

            while(offset >= stop && keys[offset] > key_max) {
                COUT_DEBUG("upper interval, offset: " << offset << ", key: " << keys[offset] << ", key_max: " << key_max);
                offset--;
            }

            notfound = offset < stop;
            if(notfound){
                segment_id--;
                segment_even = !segment_even; // flip
            }
        }

        if(offset < static_cast<ssize_t>(m_offset)){
            COUT_DEBUG("#2 - no elements qualify for the interval [" << key_min << ", " << key_max << "]");
            m_index_max = m_stop = 0;
        } else {
            m_index_max = offset+1;
            m_stop = min(m_index_max, m_stop);

            COUT_DEBUG("m_start: " << m_offset << ", m_stop: " << m_stop << ", m_index_max: " << m_index_max);
        }
    }
}

void Iterator::next_sequence() {
    assert(m_offset >= m_stop);
    size_t segment1 = m_next_segment;

    if(segment1 < m_pma.m_number_segments){
        bool segment_even = segment1 % 2 == 0;
        if(segment_even){
            m_offset = segment1 * m_pma.m_segment_capacity + m_pma.m_segment_capacity - m_pma.m_segment_sizes[segment1];
            auto segment2 = segment1 +1;
            m_stop = segment2 * m_pma.m_segment_capacity;
            if(segment2 < m_pma.m_number_segments){
                m_stop = min(m_stop + m_pma.m_segment_sizes[segment2], m_index_max);
            } else {
                m_stop = min(m_stop, m_index_max);
            }

            m_next_segment += 2;
        } else { // odd segment
            m_offset = segment1 * m_pma.m_segment_capacity;
            m_stop = min(m_index_max, m_offset + m_pma.m_segment_sizes[segment1]);
            m_next_segment++;
        }
    }
}

bool Iterator::hasNext() const {
    return m_offset < m_stop;
}

std::pair<int64_t, int64_t> Iterator::next() {
    int64_t* keys = m_pma.m_keys;
    int64_t* values = m_pma.m_values;

//    COUT_ITERATOR_NEXT("offset: " << m_offset << ", stop: " << m_stop);

    pair<int64_t, int64_t> result { keys[m_offset], values[m_offset] };

    m_offset++;
    if(m_offset >= m_stop) next_sequence();

    return result;
}

} // namespace btree_pmacc5_details

unique_ptr<pma::Iterator> BTreePMACC5::empty_iterator() const{
    return make_unique<btree_pmacc5_details::Iterator>(m_storage);
}

unique_ptr<pma::Iterator> BTreePMACC5::find(int64_t min, int64_t max) const {
    if(empty()) return empty_iterator();
    return make_unique<btree_pmacc5_details::Iterator> (m_storage, m_index.find_first(min), m_index.find_last(max), min, max );
}
unique_ptr<pma::Iterator> BTreePMACC5::iterator() const {
    if(empty()) return empty_iterator();
    return make_unique<btree_pmacc5_details::Iterator> (m_storage, 0, m_storage.m_number_segments -1,
            numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max()
    );
}

/*****************************************************************************
 *                                                                           *
 *   Aggregate sum                                                           *
 *                                                                           *
 *****************************************************************************/
pma::Interface::SumResult BTreePMACC5::sum(int64_t min, int64_t max) const {
    if((min > max) || empty()){ return SumResult{}; }
    int64_t window_start = m_index.find_first(min);
    int64_t window_end = m_index.find_last(max);
    if(window_end < window_start){ return SumResult{}; }

    int64_t* __restrict keys = m_storage.m_keys;

    bool notfound = true;
    ssize_t segment_id = window_start;
    bool segment_even = segment_id % 2 == 0;
    ssize_t start = -1, stop = -1, offset = -1;

    // start of the interval
    while(notfound && segment_id < m_storage.m_number_segments){
        if(segment_even){
            stop = (segment_id +1) * m_storage.m_segment_capacity;
            start = stop - m_storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, even segment, start: " << start << ", stop: " << stop);
        } else { // odd
            start = segment_id * m_storage.m_segment_capacity;
            stop = start + m_storage.m_segment_sizes[segment_id];
            COUT_DEBUG("lower interval, odd segment, start: " << start << ", stop: " << stop);
        }
        offset = start;

        while(offset < stop && keys[offset] < min) {
            COUT_DEBUG("lower interval, offset: " << offset << ", key: " << keys[offset] << ", key_min: " << min);
            offset++;
        }

        notfound = (offset == stop);
        if(notfound){
            segment_id++;
            segment_even = !segment_even; // flip
        }
    }

    if(segment_even && segment_id < (m_storage.m_number_segments -1)){
        stop = (segment_id +1) * m_storage.m_segment_capacity + m_storage.m_segment_sizes[segment_id +1]; // +1 implicit
    }

    if(notfound || keys[offset] > max){ return SumResult{}; }

    ssize_t end;
    { // find the last qualifying index
        assert(window_end < m_storage.m_number_segments);
        auto interval_start_segment = segment_id;
        ssize_t segment_id = window_end;
        bool segment_even = segment_id % 2 == 0;
        notfound = true;
        ssize_t offset, start, stop;

        while(notfound && segment_id >= interval_start_segment){
            if(segment_even){
                start = (segment_id +1) * m_storage.m_segment_capacity -1;
                stop = start - m_storage.m_segment_sizes[segment_id];
            } else { // odd
                stop = segment_id * m_storage.m_segment_capacity;
                start = stop + m_storage.m_segment_sizes[segment_id] -1;
            }
            COUT_DEBUG("upper interval, " << (segment_even ? "even":"odd") << " segment, start: " << start << " [key=" << keys[start] << "], stop: " << stop);
            offset = start;

            while(offset >= stop && keys[offset] > max){
                COUT_DEBUG("upper interval, offset: " << offset << ", key: " << keys[offset] << ", key_max: " << max);
                offset--;
            }

            notfound = offset < stop;
            if(notfound){
                segment_id--;
                segment_even = !segment_even; // flip
            }
        }

        end = offset +1;
    }

    if(end <= offset) return SumResult{};
    stop = std::min(stop, end);

    int64_t* __restrict values = m_storage.m_values;
    SumResult sum;
    sum.m_first_key = keys[offset];

    while(offset < end){
        sum.m_num_elements += (stop - offset);
        while(offset < stop){
            sum.m_sum_keys += keys[offset];
            sum.m_sum_values += values[offset];
            offset++;
        }

        segment_id += 1 + (segment_id % 2 == 0); // next even segment
        if(segment_id < m_storage.m_number_segments){
            ssize_t size_lhs = m_storage.m_segment_sizes[segment_id];
            ssize_t size_rhs = m_storage.m_segment_sizes[segment_id +1];
            offset = (segment_id +1) * m_storage.m_segment_capacity - size_lhs;
            stop = std::min(end, offset + size_lhs + size_rhs);
        }
    }
    sum.m_last_key = keys[end -1];

    return sum;
}


/*****************************************************************************
 *                                                                           *
 *   Bulk loading                                                            *
 *                                                                           *
 *****************************************************************************/
namespace btree_pmacc5_details {

BlkRunInfo::BlkRunInfo(uint64_t array_index, uint32_t segment_id) : m_run_start(array_index), m_run_length(1), m_cardinality(0), m_segment_start(segment_id), m_segment_length(1), m_valid(true){ }

std::ostream& operator<<(std::ostream& out, const BlkRunInfo& entry){
    out << "{run start: " << entry.m_run_start << ", length: " << entry.m_run_length << ", segment start: " << entry.m_segment_start << ", "
            "length: " << entry.m_segment_length << ", cardinality: " << entry.m_cardinality << ", valid: " << boolalpha << entry.m_valid << "}";
    return out;
}

} // namespace btree_pmacc5_details


void BTreePMACC5::load_sorted(std::pair<int64_t, int64_t>* array, size_t array_sz) {
    COUT_DEBUG("Load " << array_sz << " elements");
    if(array_sz == 0) return; // nothing to load

    if(empty()){
        // Special case, the current data structure is empty
        load_empty(array, array_sz);

    } else {
        // First, generate the runs
        auto runs = load_generate_runs(array, array_sz);

        // Second, combine the runs by checking the thresholds
        bool do_resize = load_fuse_runs(runs);

        // Third, merge the runs
        if(!do_resize){
            load_spread(array, array_sz, runs);
        // Or, alternatively, resize the whole underlying array, if we overcame the root upper threshold
        } else {
            load_resize(array, array_sz);
        }
    }

#if defined(DEBUG)
    COUT_DEBUG("Load done");
    dump();
#endif
}

BlkRunVector BTreePMACC5::load_generate_runs(std::pair<int64_t, int64_t>* array, size_t array_sz){
    std::pair<int64_t, int64_t>* __restrict A = array; // disable aliasing
    BlkRunVector runs{ m_memory_pool.allocator<BlkRunInfo>() };

    // iterate over all elements in the array
    size_t i = 0;
    while(i < array_sz){
        auto segment_id = m_index.find_first(A[i].first);
#if !defined(NDEBUG)
        int64_t min = segment_id == 0 ? numeric_limits<int64_t>::min() : get_minimum(segment_id);
#endif
        int64_t max = (segment_id +1 < m_storage.m_number_segments) ? get_minimum(segment_id +1) : numeric_limits<int64_t>::max();
        COUT_DEBUG("key: " << A[i].first << ", segment_id: " << segment_id << ", min: " << min << ", max: " << max);

        assert(min <= A[i].first && A[i].first <= max && "Invalid segment selected to place the given element");

        // Create a new run
        BlkRunInfo entry{i, static_cast<uint32_t>(segment_id)};
        i++;
        while(i < array_sz && A[i].first <= max){
            assert(A[i].first >= min && "The input array is not sorted");
            entry.m_run_length++;
            i++;
        }

        entry.m_cardinality = m_storage.m_segment_sizes[segment_id] + entry.m_run_length;
        runs.push_back(entry);
    }

    return runs;
}

bool BTreePMACC5::load_fuse_runs(BlkRunVector& runs){
    uint16_t* __restrict sizes = m_storage.m_segment_sizes;

    for(int i = 0, sz = runs.size(); i < sz; i++){
        if(!runs[i].m_valid) continue; // this run has already been fused with a previous run
        auto& run = runs[i];

        int segment_id = run.m_segment_start;
        assert(run.m_segment_length == 1 && "This run has already been manipulated/fused?");

        size_t num_elements = run.m_cardinality;
        double theta = m_density_bounds.get_upper_threshold_leaves(), density = static_cast<double>(num_elements)/m_storage.m_segment_capacity;
        size_t height = 1;
        COUT_DEBUG("run[" << i << "]: " << run << ", height: " << height << ", density: " << density << ", theta: " << theta << ", num_elements: " << num_elements);

        int window_length = 1;
        int window_id = segment_id;
        int window_start = segment_id, window_end = segment_id;

        if(m_storage.m_height > 1 && density > theta){
            // find the bounds of this window
            int windex_left = segment_id -1;
            int windex_right = segment_id +1;

            // references to the previous & next runs
            int sindex_left = i -1;
            int sindex_right = i +1;
            int srun_left = -1;
            int srun_right = -1;
            while(sindex_left >= 0 && srun_left < 0){
                if(runs[sindex_left].m_valid){
                    srun_left = runs[sindex_left].m_segment_start + runs[sindex_left].m_segment_length -1;
                } else {
                    sindex_left--;
                }
            }
            while(sindex_right < sz && srun_right < 0){
                if(runs[sindex_right].m_valid){
                    srun_right = runs[sindex_right].m_segment_start;
                } else {
                    sindex_right++;
                }
            }

            do {
                height++;
                window_length *= 2;
                window_id /= 2;
                window_start = window_id * window_length;
                window_end = window_start + window_length;

                theta = thresholds(height).second;

                while(windex_left >= window_start){ // move backwards
                    if(windex_left == srun_left){ // fuse the runs
                        num_elements += runs[sindex_left].m_cardinality;  // use the cardinality recorded in the run
                        run.m_run_start = runs[sindex_left].m_run_start;
                        run.m_run_length += runs[sindex_left].m_run_length;
                        runs[sindex_left].m_valid = false; // ignore this run
                        windex_left = static_cast<int>(runs[sindex_left].m_segment_start) -1;

                        // move to the next run
                        sindex_left--; srun_left = -1;
                        while(sindex_left >= 0 && srun_left < 0){
                            if(runs[sindex_left].m_valid){
                                srun_left = runs[sindex_left].m_segment_start + runs[sindex_left].m_segment_length -1;
                            } else {
                                sindex_left--;
                            }
                        }
                    } else {
                        num_elements += sizes[windex_left];
                        windex_left--;
                    }
                }
                while(windex_right < window_end){ // move forewards
                    if(windex_right == srun_right){ // fuse the runs
                        num_elements += runs[sindex_right].m_cardinality;
                        run.m_run_length += runs[sindex_right].m_run_length;
                        runs[sindex_right].m_valid = false; // ignore this run hereafter
                        windex_right = runs[sindex_right].m_segment_start + runs[sindex_right].m_segment_length;

                        // move to the next run
                        sindex_right++; srun_right = -1;
                        while(sindex_right < sz && srun_right < 0){
                            if(runs[sindex_right].m_valid){
                                srun_right = runs[sindex_right].m_segment_start;
                            } else {
                                sindex_right++;
                            }
                        }
                    } else {
                        num_elements += sizes[windex_right];
                        windex_right++;
                    }
                }

                COUT_DEBUG("run: " << run << ", num_elements: " << num_elements << ", window_start: " << window_start << ",  window_length: " << window_length << ",  segment_capacity: " << m_storage.m_segment_capacity);
                density = ((double) num_elements) / (window_length * m_storage.m_segment_capacity);

                COUT_DEBUG("height: " << height << ", density: " << density << ", theta: " << theta);
            } while ((density > theta) && height < m_storage.m_height);
        }

        // update the start position of this segment
        run.m_segment_start = window_start;
        run.m_segment_length = window_length;
        run.m_cardinality = num_elements;

        // there is no point to continue in fusing the runs, here we need to resize the whole underlying array anyway
        if(window_length == m_storage.m_number_segments && density > theta){
            return true;
        }
    }

#if defined(DEBUG)
    COUT_DEBUG("Final runs: ");
    for(size_t i = 0; i < runs.size(); i++){
        if(!runs[i].m_valid) continue;
        cout << "[" << i << "] " << runs[i] << "\n";
    }
#endif

    return false; // no need to resize the underlying array
}

void BTreePMACC5::load_spread(pair<int64_t, int64_t>* __restrict array, size_t array_sz, const BlkRunVector& runs){
    for(size_t i = 0, sz = runs.size(); i < sz; i++){
        if(!runs[i].m_valid) continue; // ignore this run, it has been superseded by another run
        const auto& entry = runs[i];

        if(entry.m_segment_length == 1){ // single segment
            if(entry.m_run_length == 1){ // single element, no rebalance is necessary because entry.m_segment_length == 1
                if(UNLIKELY(empty())){
                    insert_empty(array[entry.m_run_start].first, array[entry.m_run_start].second);
                } else {
                    insert_common(entry.m_segment_start, array[entry.m_run_start].first, array[entry.m_run_start].second);
                }
            } else if (entry.m_segment_length == 1){ // multiple elements, but single segment
                load_merge_single(entry.m_segment_start, array + entry.m_run_start, entry.m_run_length, entry.m_cardinality);
            }
        } else { // multiple segments
            load_merge_multi(entry.m_segment_start, entry.m_segment_length, array + entry.m_run_start, entry.m_run_length, entry.m_cardinality);
        }
    }
}

void BTreePMACC5::load_merge_single(size_t segment_id, std::pair<int64_t, int64_t>* __restrict sequence, size_t sequence_size, size_t cardinality){
    COUT_DEBUG("segment_id: " << segment_id << ", segment size: " << m_storage.m_segment_sizes[segment_id] << ", sequence size: " << sequence_size << ", total cardinality: " << cardinality);

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    segment_size_t* __restrict sizes = m_storage.m_segment_sizes;
    int64_t* __restrict output_keys = m_storage.m_keys + segment_id * m_storage.m_segment_capacity;
    int64_t* __restrict output_values = m_storage.m_values + segment_id * m_storage.m_segment_capacity;

    // temporary arrays
    const size_t input_size = sizes[segment_id];
    assert(input_size == cardinality - sequence_size && "Cardinality should be the sum of the run and current size of the segment");
    auto& memory_pool = m_memory_pool;
    auto memory_pool_deleter = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_keys_ptr { m_memory_pool.allocate<int64_t>(input_size), memory_pool_deleter };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_values_ptr {  m_memory_pool.allocate<int64_t>(input_size), memory_pool_deleter };
    int64_t* __restrict input_keys = input_keys_ptr.get();
    int64_t* __restrict input_values = input_values_ptr.get();

    { // move the current elements into the temporary arrays
        size_t start = (segment_id % 2 == 0) ? m_storage.m_segment_capacity - input_size : 0;
        memcpy(input_keys, output_keys + start, input_size * sizeof(input_keys[0]));
        memcpy(input_values, output_values + start, input_size * sizeof(input_values[0]));
    }

    // debug only
#if defined(DEBUG)
    for(size_t i = 0; i < input_size; i++){
        cout << "PMA [" << i << "] <" << input_keys[i] << ", " << input_values[i] << ">\n";
    }
    for(size_t i = 0; i < sequence_size; i++){
        cout << "Batch [" << i << "] <" << sequence[i].first << ", " << sequence[i].second << ">\n";
    }
    std::flush(cout);
#endif

    // merge the existing elements with the sequence being loaded
    {
        size_t output_start = (segment_id % 2 == 0) ? m_storage.m_segment_capacity - cardinality : 0;
        size_t output_current = output_start;
        size_t output_end = (segment_id % 2 == 0) ? m_storage.m_segment_capacity : cardinality;
        size_t input_current = 0;
        size_t sequence_current = 0;

        // merge from both the PMA and the user sequence
        while(output_current < output_end && input_current < input_size && sequence_current < sequence_size){
            if(sequence[sequence_current].first < input_keys[input_current] ){
                output_keys[output_current] = sequence[sequence_current].first;
                output_values[output_current] = sequence[sequence_current].second;
                sequence_current++;
            } else {
                output_keys[output_current] = input_keys[input_current];
                output_values[output_current] = input_values[input_current];
                input_current++;
            }
            output_current++;
        }
        // only copy from the PMA
        if(output_current < output_end && input_current < input_size){
            assert((output_end - output_current) == (input_size - input_current) && "Missing elements to copy");
            size_t elements2copy = output_end - output_current;
            memcpy(output_keys + output_current, input_keys + input_current, elements2copy * sizeof(input_keys[0]));
            memcpy(output_values + output_current, input_values + input_current, elements2copy * sizeof(input_values[0]));
            output_current += elements2copy; // redundant, only for validation purposes
            input_current += elements2copy; // redundant
        }
        // only copy from the sequence being loaded
        /* else */
        while(output_current < output_end && sequence_current < sequence_size){
            output_keys[output_current] = sequence[sequence_current].first;
            output_values[output_current] = sequence[sequence_current].second;
            sequence_current++;
            output_current++;
        }
        assert(output_current == output_end && "All elements should have been merged");

        // update the separator key
        m_index.set_separator_key(segment_id, output_keys[output_start]);

        sizes[segment_id] = cardinality;
        m_storage.m_cardinality += sequence_size;
    }
}

void BTreePMACC5::load_merge_multi(size_t segment_start, size_t segment_length, std::pair<int64_t, int64_t>* __restrict sequence, size_t sequence_sz, size_t cardinality){
    COUT_DEBUG("segment_start: " << segment_start << ", segment_length: " << segment_length << ", run size: " << sequence_sz << ", cardinality: " << cardinality);
    assert(segment_start % 2 == 0 && "Expected an even segment");
    assert(segment_length > 1 && "Expected to merge on multiple segments. For a single segment use `load_merge_single'");
    assert(segment_length * m_storage.m_segment_capacity >= cardinality && "Not enough space to store `cardinality' elements in the current window");

    // the new cardinality of each segment
    const size_t elements_per_segment = cardinality / segment_length;
    const size_t num_odd_segments = cardinality % segment_length;

    // workspace
    using segment_size_t = remove_pointer_t<decltype(m_storage.m_segment_sizes)>;
    segment_size_t* __restrict sizes = m_storage.m_segment_sizes + segment_start;
    int64_t* __restrict output_keys = m_storage.m_keys + segment_start * m_storage.m_segment_capacity;
    int64_t* __restrict output_values = m_storage.m_values + segment_start * m_storage.m_segment_capacity;

    // input chunk 2 (extra space)
    const size_t input_chunk2_capacity = (static_cast<size_t>(m_storage.m_segment_capacity) + (segment_length / (elements_per_segment +1))) *2;
    size_t input_chunk2_size = 0;
    auto& memory_pool = m_memory_pool;
    auto memory_pool_deleter = [&memory_pool](void* ptr){ memory_pool.deallocate(ptr); };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_chunk2_keys_ptr { m_memory_pool.allocate<int64_t>(input_chunk2_capacity), memory_pool_deleter };
    unique_ptr<int64_t, decltype(memory_pool_deleter)> input_chunk2_values_ptr {  m_memory_pool.allocate<int64_t>(input_chunk2_capacity), memory_pool_deleter };
    int64_t* __restrict input_chunk2_keys = input_chunk2_keys_ptr.get();
    int64_t* __restrict input_chunk2_values = input_chunk2_values_ptr.get();

    // input chunk1 (it overlaps the current window)
    int64_t* __restrict input_chunk1_keys = nullptr;
    int64_t* __restrict input_chunk1_values = nullptr;
    size_t input_chunk1_size = 0;

    { // 1) first compact all elements towards the end
        int64_t output_segment_id = segment_length -2;
        int64_t output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
        int64_t output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
        int64_t output_current = output_end;

        // copy up to `input_chunk2_capacity' elements into the new array
        int64_t input_chunk2_space_left = input_chunk2_capacity;
        while(output_segment_id >= 0 && input_chunk2_space_left > 0){
            size_t elements2copy = min(input_chunk2_space_left, output_current - output_start);
            memcpy(input_chunk2_keys + input_chunk2_space_left - elements2copy, output_keys + output_current - elements2copy, elements2copy * sizeof(input_chunk2_keys[0]));
            memcpy(input_chunk2_values + input_chunk2_space_left - elements2copy, output_values + output_current - elements2copy, elements2copy * sizeof(input_chunk2_values[0]));

            output_current -= elements2copy;
            input_chunk2_space_left -= elements2copy;

            // fetch the next chunk
            if(output_current <= output_start){
                output_segment_id -= 2;
                if(output_segment_id >= 0){
                    output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
                    output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
                    output_current = output_end;
                }
            }
        }

        // readjust the pointers for input_chunk2
        input_chunk2_size = input_chunk2_capacity - input_chunk2_space_left;
        input_chunk2_keys += input_chunk2_space_left;
        input_chunk2_values += input_chunk2_space_left;

        // move the remaining elements towards the end of the array
        int64_t input_chunk1_current = segment_length * m_storage.m_segment_capacity;
        while(output_segment_id >= 0){
            size_t elements2copy = output_current - output_start;
            memcpy(output_keys + input_chunk1_current - elements2copy, output_keys + output_current - elements2copy, elements2copy * sizeof(output_keys[0]));
            memcpy(output_values + input_chunk1_current - elements2copy, output_values + output_current - elements2copy, elements2copy * sizeof(output_values[0]));

            input_chunk1_current -= elements2copy;
            output_current -= elements2copy;

            // fetch the next chunk
            if(output_current <= output_start){
                output_segment_id -= 2;
                if(output_segment_id >= 0){
                    output_start = (output_segment_id +1) * m_storage.m_segment_capacity - sizes[output_segment_id];
                    output_end = output_start + sizes[output_segment_id] + sizes[output_segment_id +1];
                    output_current = output_end;
                }
            }
        }

        // readjust the pointers for input_chunk1
        input_chunk1_size = segment_length * m_storage.m_segment_capacity - input_chunk1_current;
        input_chunk1_keys = output_keys + input_chunk1_current;
        input_chunk1_values = output_values + input_chunk1_current;
    }

    // debug only
#if defined(DEBUG)
    size_t k = 0;
    for(size_t i = 0; i < input_chunk1_size; i++){
        cout << "Chunk1 [" << k++ << "] <" << input_chunk1_keys[i] << ", " << input_chunk1_values[i] << ">\n";
    }
    for(size_t i = 0; i < input_chunk2_size; i++){
        cout << "Chunk2 [" << k++ << "] <" << input_chunk2_keys[i] << ", " << input_chunk2_values[i] << ">\n";
    }
    for(size_t i = 0; i < sequence_sz; i++){
        cout << "Batch [" << i << "] <" << sequence[i].first << ", " << sequence[i].second << ">\n";
    }
    std::flush(cout);
#endif

    // 2) set the expected size of each segment
    for(size_t i = 0; i < segment_length; i++){
        sizes[i] = elements_per_segment + (i < num_odd_segments);
    }

    // 3) initialise the input chunk
    int64_t* __restrict input_keys;
    int64_t* __restrict input_values;
    size_t input_current = 0;
    size_t input_size;
    if(input_chunk1_size > 0){
        input_keys = input_chunk1_keys;
        input_values = input_chunk1_values;
        input_size = input_chunk1_size;
    } else {
        input_keys = input_chunk2_keys;
        input_values = input_chunk2_values;
        input_size = input_chunk2_size;
    }
    size_t sequence_current = 0;

    // 4) merge the elements from the PMA & the batch sequence being loaded
    for(size_t i = 0; i < segment_length; i+=2){
        size_t output_start = (i +1) * m_storage.m_segment_capacity - sizes[i];
        size_t output_end = output_start + sizes[i] + sizes[i+1];
        size_t output_current = output_start;
        COUT_DEBUG("segment id: " << i << ", element_to_copy: " << output_end - output_start);

        // merge from both the PMA & the user batch
        while(output_current < output_end && input_current < input_size && sequence_current < sequence_sz){
            COUT_DEBUG("<merge> output_start: " << output_start << ", output_end: " << output_end << ", output_current: " << output_current << ", pma key: " << input_keys[input_current] << ", sequence key: " << sequence[sequence_current].first);
            if(input_keys[input_current] <= sequence[sequence_current].first){
                output_keys[output_current] = input_keys[input_current];
                output_values[output_current] = input_values[input_current];
                input_current++;

                // switch to the second chunk
                if(input_current == input_size && input_keys == input_chunk1_keys){
                    input_keys = input_chunk2_keys;
                    input_values = input_chunk2_values;
                    input_size = input_chunk2_size;
                    input_current = 0;
                }
            } else {
                output_keys[output_current] = sequence[sequence_current].first;
                output_values[output_current] = sequence[sequence_current].second;
                sequence_current++;
            }
            output_current++;
        }
        // only merge from the PMA
        while(output_current < output_end && input_current < input_size){
            size_t elements2copy = min(output_end - output_current, input_size - input_current);
            memcpy(output_keys + output_current, input_keys + input_current, elements2copy * sizeof(output_keys[0]));
            memcpy(output_values + output_current, input_values + input_current, elements2copy * sizeof(output_values[0]));
            output_current += elements2copy;
            input_current += elements2copy;
            // switch to the second chunk
            if(input_current == input_size && input_keys == input_chunk1_keys){
                input_keys = input_chunk2_keys;
                input_values = input_chunk2_values;
                input_size = input_chunk2_size;
                input_current = 0;
            }
        }
        // only merge from the user batch
        while(output_current < output_end && sequence_current < sequence_sz){
            output_keys[output_current] = sequence[sequence_current].first;
            output_values[output_current] = sequence[sequence_current].second;
            sequence_current++;
            output_current++;
        }

        // update the separator keys
        m_index.set_separator_key(segment_start + i, output_keys[output_start]);
        m_index.set_separator_key(segment_start + i + 1, output_keys[output_start + sizes[i]]);
    }

    m_storage.m_cardinality += sequence_sz;
}

void BTreePMACC5::load_resize(std::pair<int64_t, int64_t>* __restrict batch, size_t batch_size) {
    const double target_density = m_density_bounds.get_upper_threshold_root();
    const size_t cardinality = m_storage.m_cardinality + batch_size;
    const size_t capacity = hyperceil( ceil(cardinality / target_density) );
    const size_t num_segments = capacity / m_storage.m_segment_capacity;
    const size_t elements_per_segment = cardinality / num_segments;
    const size_t odd_segments = cardinality % num_segments;
    COUT_DEBUG(m_storage.m_capacity << " --> " << capacity << ", cardinality: " << cardinality << ", num_segments: " << num_segments << ", elements per segment: " << elements_per_segment << ", odd segments: " << odd_segments);

    // rebuild the PMAs
    int64_t* ixKeys;
    int64_t* ixValues;
    decltype(m_storage.m_segment_sizes) ixSizes;
    m_storage.alloc_workspace(num_segments, &ixKeys, &ixValues, &ixSizes);
    // swap the pointers with the previous workspace
    swap(ixKeys, m_storage.m_keys);
    swap(ixValues, m_storage.m_values);
    swap(ixSizes, m_storage.m_segment_sizes);
    auto xFreePtr = [](void* ptr){ free(ptr); };
    unique_ptr<int64_t, decltype(xFreePtr)> ixKeys_ptr { ixKeys, xFreePtr };
    unique_ptr<int64_t, decltype(xFreePtr)> ixValues_ptr{ ixValues, xFreePtr };
    unique_ptr<remove_pointer_t<decltype(m_storage.m_segment_sizes)>, decltype(xFreePtr)> ixSizes_ptr{ ixSizes, xFreePtr };
    int64_t* __restrict output_keys = m_storage.m_keys;
    int64_t* __restrict output_values = m_storage.m_values;
    decltype(m_storage.m_segment_sizes) __restrict output_sizes = m_storage.m_segment_sizes;

    m_index.rebuild(num_segments);

    // input elements
    size_t input_segment_id = 0;
    size_t input_current = m_storage.m_segment_capacity - ixSizes[0];
    size_t input_end = m_storage.m_segment_capacity + ixSizes[1];
    int64_t* __restrict input_keys = ixKeys;
    int64_t* __restrict input_values = ixValues;
    size_t batch_current = 0; // current position in the array `batch'

    // start copying the elements
    for(size_t j = 0; j < num_segments; j+=2){
        // new cardinality for the given segments
        output_sizes[j] = elements_per_segment + (j < odd_segments);
        output_sizes[j+1] = elements_per_segment + ((j +1) < odd_segments);

        // output start & stop positions
        size_t output_start = m_storage.m_segment_capacity * (j+1) - output_sizes[j];
        size_t output_current = output_start;
        size_t output_end = output_current + output_sizes[j] + output_sizes[j+1];

//        COUT_DEBUG("segments: [" << j << ", " << j+1 << "], output_start: " << output_start << ", output_end: " << output_end);

        // merge from both the underlying PMA and the loaded array
        while(output_current < output_end && batch_current < batch_size && input_current < input_end){
//            COUT_DEBUG("<merge> output_current: " << output_current << ", key PMA: " << input_keys[input_current] << ", key batch: " << batch[batch_current].first);
            if(input_keys[input_current] < batch[batch_current].first){ // fetch the next element from the PMA
                output_keys[output_current] = input_keys[input_current];
                output_values[output_current] = input_values[input_current];
                input_current++;

                if(input_current >= input_end){ // move to the next input chunk
                    input_segment_id += 2;
                    if(input_segment_id < m_storage.m_number_segments){
                        input_current = m_storage.m_segment_capacity * (input_segment_id +1) - ixSizes[input_segment_id];
                        input_end = input_current + ixSizes[input_segment_id] + ixSizes[input_segment_id +1];
                    }
                }

            } else { // fetch the next element from the batch being loaded
                output_keys[output_current] = batch[batch_current].first;
                output_values[output_current] = batch[batch_current].second;
                batch_current++;
            }

            output_current++;
        }

        // only copy from the PMA
        while(output_current < output_end && input_current < input_end){
            size_t elements2copy = min(output_end - output_current, input_end - input_current);
//            COUT_DEBUG("<pma> output_current: " << output_current << ", input_current: " << input_current << ", input_end: " << input_end << ", elements2copy: " << elements2copy);
            memcpy(output_keys + output_current, input_keys + input_current, elements2copy * sizeof(output_keys[0]));
            memcpy(output_values + output_current, input_values + input_current, elements2copy * sizeof(output_values[0]));

            input_current += elements2copy;
            output_current += elements2copy;

            if(input_current >= input_end){ // move to the next input chunk
                input_segment_id += 2;
                if(input_segment_id < m_storage.m_number_segments){
                    input_current = m_storage.m_segment_capacity * (input_segment_id +1) - ixSizes[input_segment_id];
                    input_end = input_current + ixSizes[input_segment_id] + ixSizes[input_segment_id +1];
                }
            }
        }

        // only copy from the elements being loaded
        if(output_current < output_end && batch_current < batch_size){
//            COUT_DEBUG("<batch> output_current: " << output_current << ", batch index: " << batch_current << ", batch size: " << batch_size);
            assert((output_end - output_current) <= (batch_size - batch_current) && "Missing elements to copy");
            while(output_current < output_end){
                output_keys[output_current] = batch[batch_current].first;
                output_values[output_current] = batch[batch_current].second;
                output_current++;
                batch_current++;
            }
        }

        // update the separator keys in the static index
        m_index.set_separator_key(j, output_keys[output_start] );
        m_index.set_separator_key(j+1, output_keys[output_start + output_sizes[j]]);
    }

    // update the PMA properties
    m_storage.m_cardinality = cardinality;
    m_storage.m_capacity = capacity;
    m_storage.m_number_segments = num_segments;
    m_storage.m_height = log2(num_segments) +1;

    // side effect: regenerate the thresholds
    thresholds(m_storage.m_height, m_storage.m_height);
}

void BTreePMACC5::load_empty(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz){
    assert(array_sz > 0 && "Empty batch");
    assert(empty() && "The container should be empty");

    if(m_storage.m_segment_capacity * m_density_bounds.get_upper_threshold_leaves() >= array_sz){
        load_empty_single(array, array_sz);
    } else {
        load_empty_multi(array, array_sz);
    }
}

void BTreePMACC5::load_empty_single(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz){
    assert(m_storage.m_number_segments == 1 && "Expected to have only a segment");
    assert(m_storage.m_segment_capacity >= array_sz && "Not enough room to store `array_sz' elements");

    size_t output_start = m_storage.m_segment_capacity - array_sz;

    for(size_t i = 0, j = output_start; i < array_sz; i++, j++){
        m_storage.m_keys[j] = array[i].first;
        m_storage.m_values[j] = array[i].second;
    }

    m_index.set_separator_key(0, array[0].first);
    m_storage.m_segment_sizes[0] = array_sz;
    m_storage.m_cardinality = array_sz;
}

void BTreePMACC5::load_empty_multi(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz){
    assert(empty() && "Invoke this method only when the data structure is empty");
    // as we are going to resize, we need to pick a density of the segments. An option would be to use the maximum density (=1), but that may require
    // to resize immediately after a ::load with a single insertions, if all segments become full. As compromise, we use the average density between
    // the root and the lowest levels of the calibrator tree
    const double target_density = (m_density_bounds.get_upper_threshold_root() + m_density_bounds.get_upper_threshold_leaves()) / 2;
    const size_t capacity = hyperceil( ceil(array_sz / target_density) );
    const size_t num_segments = capacity / m_storage.m_segment_capacity;
    const size_t elements_per_segment = array_sz / num_segments;
    const size_t odd_segments = array_sz % num_segments;
    COUT_DEBUG("batch size: " << array_sz << ", target density: " << target_density << ", PMA capacity: " << capacity << ", num segments: " << num_segments << ", elements per segment: " << elements_per_segment << ", odd segments: " << odd_segments);

    // 1) build the index
    m_index.rebuild(num_segments);

    // 2) build the PMA
    free(m_storage.m_segment_sizes);
    free(m_storage.m_keys);
    free(m_storage.m_values);
    m_storage.alloc_workspace(num_segments, &m_storage.m_keys, &m_storage.m_values, &m_storage.m_segment_sizes);
    int64_t* __restrict output_keys = m_storage.m_keys;
    int64_t* __restrict output_values = m_storage.m_values;
    decltype(m_storage.m_segment_sizes) __restrict output_sizes = m_storage.m_segment_sizes;

    // 3) set the size of each segment
    for(size_t i = 0; i < num_segments; i++){
        output_sizes[i] = elements_per_segment + (i < odd_segments);
    }

    // 4) copy the elements into the sparse arrays
    size_t array_current = 0;
    for(size_t i = 0; i < num_segments; i+= 2){
        const size_t output_start = (i+1) * m_storage.m_segment_capacity - output_sizes[i];
        const size_t output_end = output_start + output_sizes[i] + output_sizes[i+1];

        for(size_t output_current = output_start; output_current < output_end; output_current++){
            output_keys[output_current] = array[array_current].first;
            output_values[output_current] = array[array_current].second;
            array_current++;
        }

        // update the separator keys in the static index
        m_index.set_separator_key(i, output_keys[output_start]);
        m_index.set_separator_key(i+1, output_keys[output_start + output_sizes[i]]);
    }
    assert(array_current == array_sz && "All elements should have been copied");

    // 5) update the PMA properties
    m_storage.m_cardinality = array_sz;
    m_storage.m_capacity = capacity;
    m_storage.m_number_segments = num_segments;
    m_storage.m_height = log2(num_segments) +1;

    // side effect: regenerate the thresholds
    thresholds(m_storage.m_height, m_storage.m_height);
}

int64_t BTreePMACC5::get_minimum(size_t segment_id) const {
    int64_t* __restrict keys = m_storage.m_keys;
    auto* __restrict sizes = m_storage.m_segment_sizes;

    assert(segment_id < m_storage.m_number_segments && "Invalid segment");
    assert(sizes[segment_id] > 0 && "The segment is empty!");

    if(segment_id % 2 == 0){ // even segment
        return keys[(segment_id +1) * m_storage.m_segment_capacity - sizes[segment_id]];
    } else { // odd segment
        return keys[segment_id * m_storage.m_segment_capacity];
    }
}

/*****************************************************************************
 *                                                                           *
 *   Segment statistics                                                      *
 *                                                                           *
 *****************************************************************************/
namespace {
    struct SegmentStatistics {
        uint64_t m_num_segments;
        uint64_t m_distance_avg;
        uint64_t m_distance_min;
        uint64_t m_distance_max;
        uint64_t m_distance_stddev;
        uint64_t m_distance_median;
        uint64_t m_cardinality_avg;
        uint64_t m_cardinality_min;
        uint64_t m_cardinality_max;
        uint64_t m_cardinality_stddev;
        uint64_t m_cardinality_median;
    };
}

decltype(auto) BTreePMACC5::compute_segment_statistics() const {
    SegmentStatistics stats {m_storage.m_number_segments, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    // memory distances
    uint64_t distance_sum = 0;
    uint64_t distance_sum_sq = 0;
    uint64_t distance_min = -1;
    uint64_t distance_max = 0;
    uint64_t distance_gap_start = 0;
    vector<uint64_t> distances;
    distances.reserve(m_storage.m_number_segments / 2 /*-1*/);

    // cardinalities
    uint64_t cardinality_sum = m_storage.m_cardinality;
    uint64_t cardinality_sum_sq = 0;
    uint64_t cardinality_min = -1;
    uint64_t cardinality_max = 0;
    vector<uint64_t> cardinalities;
    cardinalities.reserve(m_storage.m_number_segments);


    for(size_t i = 0; i < m_storage.m_number_segments; i++){
        size_t segment_size = m_storage.m_segment_sizes[i];

        // memory distances
        if(i > 0){
            if (i % 2 == 0){
                uint64_t distance_gap_end = 2 * m_storage.m_segment_capacity - segment_size;
                uint64_t distance_current = (distance_gap_end - distance_gap_start) * sizeof(m_storage.m_keys[0]);
                distance_sum += distance_current;
                distance_sum_sq += distance_current * distance_current;
                if(distance_min > distance_current) distance_min = distance_current;
                if(distance_max < distance_current) distance_max = distance_current;
                distances.push_back(distance_current);
            } else { // odd segment
                distance_gap_start = segment_size;
            }
        }

        // cardinalities
        cardinality_sum_sq += (segment_size * segment_size);
        if(cardinality_min > segment_size) cardinality_min = segment_size;
        if(cardinality_max < segment_size) cardinality_max = segment_size;
        cardinalities.push_back(segment_size);
    }

    // distances
    size_t dist_sz = distances.size();
    if(dist_sz > 0){
        stats.m_distance_avg = distance_sum / dist_sz;
        stats.m_distance_max = distance_max;
        stats.m_distance_min = distance_min;
        stats.m_distance_stddev = sqrt( (static_cast<double>(distance_sum_sq) / dist_sz) -
                pow(stats.m_distance_avg, 2.0) );
        sort(begin(distances), end(distances));
        assert(dist_sz == m_storage.m_number_segments /2 -1);
        if(dist_sz % 2 == 1){
            stats.m_distance_median = distances[dist_sz /2];
        } else {
            size_t d1 = dist_sz /2;
            size_t d0 = d1 - 1;
            stats.m_distance_median = (distances[d0] + distances[d1]) / 2;
        }
    }

    // cardinalities
    stats.m_cardinality_avg = cardinality_sum / m_storage.m_number_segments;
    stats.m_cardinality_max = cardinality_max;
    stats.m_cardinality_min = cardinality_min;
    stats.m_cardinality_stddev = sqrt( (static_cast<double>(cardinality_sum_sq) / m_storage.m_number_segments) -
            pow(stats.m_cardinality_avg, 2.0) );

    // Compute the median
    sort(begin(cardinalities), end(cardinalities));
    size_t card_sz = cardinalities.size();
    assert(card_sz == m_storage.m_number_segments);
    if(card_sz % 2 == 1){
        stats.m_cardinality_median = cardinalities[card_sz /2];
    } else {
        size_t d1 = card_sz /2;
        size_t d0 = d1 - 1;
        stats.m_cardinality_median = (cardinalities[d0] + cardinalities[d1]) / 2;
    }

    return stats;
}

void BTreePMACC5::record_segment_statistics() const {
    LOG_VERBOSE("[btreecc_pma5] Computing segment statistics...");

    auto stats = compute_segment_statistics();

    LOG_VERBOSE("--> # segments: " << stats.m_num_segments);
    LOG_VERBOSE("--> distance average: " << stats.m_distance_avg << ", min: " << stats.m_distance_min << ", max: " << stats.m_distance_max << ", std. dev: " <<
            stats.m_distance_stddev << ", median: " << stats.m_distance_median);
    LOG_VERBOSE("--> cardinality average: " << stats.m_cardinality_avg << ", min: " << stats.m_cardinality_min << ", max: " << stats.m_cardinality_max << ", std. dev: " <<
            stats.m_cardinality_stddev << ", median: " << stats.m_cardinality_median);

    config().db()->add("btree_leaf_statistics")
                    ("num_leaves", stats.m_num_segments)
                    ("dist_avg", stats.m_distance_avg)
                    ("dist_min", stats.m_distance_min)
                    ("dist_max", stats.m_distance_max)
                    ("dist_stddev", stats.m_distance_stddev)
                    ("dist_median", stats.m_distance_median)
                    ("card_avg", stats.m_cardinality_avg)
                    ("card_min", stats.m_cardinality_min)
                    ("card_max", stats.m_cardinality_max)
                    ("card_stddev", stats.m_cardinality_stddev)
                    ("card_median", stats.m_cardinality_median)
                    ;
}

void BTreePMACC5::set_record_segment_statistics(bool value) {
    m_segment_statistics = value;
}


/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/
void BTreePMACC5::dump() const {
    dump(std::cout);
}

void BTreePMACC5::dump(std::ostream& out) const {
    bool integrity_check = true;

    m_index.dump(out, &integrity_check);

    out << "\n";

    dump_storage(out, &integrity_check);

    assert(integrity_check && "Integrity check failed!");
}

void BTreePMACC5::dump_storage(std::ostream& out, bool* integrity_check) const {
    cout << "[PMA] cardinality: " << m_storage.m_cardinality << ", capacity: " << m_storage.m_capacity << ", " <<
            "height: "<< m_storage.m_height << ", #segments: " << m_storage.m_number_segments <<
            ", blksz #elements: " << m_storage.m_segment_capacity << endl;

    if(empty()){ // edge case
        cout << "-- empty --" << endl;
        return;
    }

    int64_t previous_key = numeric_limits<int64_t>::min();

    int64_t* keys = m_storage.m_keys;
    int64_t* values = m_storage.m_values;
    auto sizes = m_storage.m_segment_sizes;
    size_t tot_count = 0;

    for(size_t i = 0; i < m_storage.m_number_segments; i++){
        out << "[" << i << "] ";

        tot_count += sizes[i];
        bool even = i % 2 == 0;
        size_t start = even ? m_storage.m_segment_capacity - sizes[i] : 0;
        size_t end = even ? m_storage.m_segment_capacity : sizes[i];

        for(size_t j = start, sz = end; j < sz; j++){
            if(j > start) out << ", ";
            out << "<" << keys[j] << ", " << values[j] << ">";

            // sanity check
            if(keys[j] < previous_key){
                out << " (ERROR: order mismatch: " << previous_key << " > " << keys[j] << ")";
                if(integrity_check) *integrity_check = false;
            }
            previous_key = keys[j];
        }
        out << endl;

        if(keys[start] != m_index.get_separator_key(i)){
            out << " (ERROR: invalid pivot, minimum: " << keys[start] << ", pivot: " << m_index.get_separator_key(i) <<  ")" << endl;
            if(integrity_check) *integrity_check = false;
        }

        // next segment
        keys += m_storage.m_segment_capacity;
        values += m_storage.m_segment_capacity;
    }

    if(m_storage.m_cardinality != tot_count){
        out << " (ERROR: size mismatch, pma registered cardinality: " << m_storage.m_cardinality << ", computed cardinality: " << tot_count <<  ")" << endl;
        if(integrity_check) *integrity_check = false;
    }
}

} // namespace pma
