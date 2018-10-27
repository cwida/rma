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

#include "range_query.hpp"

#include <cassert>
#include <memory>
#include <random>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "database.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp"
#include "profiler.hpp"
#include "distribution/distribution.hpp"
#include "distribution/driver.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"

using namespace distribution;
using namespace std;

#define REPORT_TIME( preamble, time ) { cout << preamble << ' '; \
    if( time > 3000 ) { cout << (static_cast<double>(time) / 1000) << " seconds"; } \
    else { cout << time << " milliseconds"; } }

#if !defined(PROFILING)
#define REPORT_TIME_PROFILED(preamble, time) { REPORT_TIME(preamble, time); cout << endl; }
#else
#define REPORT_TIME_PROFILED(preamble, time ) { REPORT_TIME(preamble, time); cout << ", " << profiler_snapshot << endl; }
#endif

namespace pma {

namespace {

/**
 * Base class for the container keys.
 */
struct ContainerKeys {
public:
    virtual ~ContainerKeys() { }

    // The number of keys in the container
    virtual uint64_t size() const = 0;

    // Get the key at the given position
    virtual int64_t key_at(size_t pos) const = 0;
    int64_t at(size_t pos) const { return key_at(pos); } // alias

    // Get the expected the sum of the keys for a scan in the interval [pos_min, pos_max]
    virtual uint64_t expected_sum(size_t pos_min, size_t pos_max) const = 0;
};

/**
 * Dense container. All the keys in [min, min + length), i.e. [min, min+1, min+2, ..., min + length -1]
 */
class ContainerKeysDense : public ContainerKeys {
    const int64_t m_minimum;
    const size_t m_length;

public:
    ContainerKeysDense(int64_t min, size_t length): m_minimum(min), m_length(length){ }

    uint64_t size() const { return m_length; }
    int64_t key_at(size_t pos) const {
        assert(pos < size());
        return m_minimum + (int64_t) pos;
    }
    uint64_t expected_sum(size_t pos_min, size_t pos_max) const {
        int64_t min = key_at(pos_min);
        int64_t max = key_at(pos_max);
        return (max * (max+1) - (min) * (min-1)) /2;
    }
};

/**
 * Sparse container. To validate it, we need to gather all keys container and build a prefix sum for validation purposes.
 * With 1G keys, it needs ~16GB of space.
 */
class ContainerKeysSparse : public ContainerKeys {
    vector<pair<int64_t, uint64_t>> m_keys;

public:

    ContainerKeysSparse(Interface* pma){
        m_keys.reserve(pma->size());
        auto it = pma->iterator();
        int64_t i = 0;
        while(it->hasNext()){
            int64_t key = it->next().first;

            int64_t prefix_sum = key;
            if(i>0) prefix_sum += m_keys[i-1].second;

            m_keys.emplace_back(key, prefix_sum);
            i++;
        }
    }

    uint64_t size() const { return m_keys.size(); }

    int64_t key_at(size_t pos) const {
        assert(pos < size());
        return m_keys.at(pos).first;
    }

    uint64_t expected_sum(size_t pos_min, size_t pos_max) const {
        assert(pos_min <= pos_max && pos_max < size()); // range checks

        uint64_t result = m_keys[pos_max].second;
        if(pos_min > 0) result -= m_keys[pos_min -1].second;

        return result;
    }
};


/**
 * Measure the range query time for the given structure. Assume that the data
 * structure contains the all the keys in the sequence [1, N]
 */
struct ExperimentRangeQueryImpl : public Experiment {
    const Interface* pma;
    const double interval_sz; // 0 = no elements, 0.5 = 50% of the elements, 1 = the whole interval [1, N]
    const size_t numLookups; // how many range queries to perform
    const size_t N; // last element in the pma (max)
    const size_t seed; // the random number generator for the look ups
    const ContainerKeys* m_keys; // keys contained

    ExperimentRangeQueryImpl(const Interface* pma, double interval_sz, size_t numLookups, size_t max, size_t seed, const ContainerKeys* keys) :
        pma(pma), interval_sz(interval_sz), numLookups(numLookups), N(max), seed(seed),
        m_keys(keys){
        if(!pma) throw std::invalid_argument("Null pointer");
        if(N <= 1) throw std::invalid_argument("N <= 1");
        if(numLookups <= 1) throw std::invalid_argument("numLookups <= 1");
    };

    virtual void validate(const ::pma::Interface::SumResult& sum, size_t pos_min, size_t pos_max){
        uint64_t expected_sum = m_keys->expected_sum(pos_min, pos_max);

        if(sum.m_sum_keys != expected_sum){
            RAISE_EXCEPTION(Exception, "[SANITY CHECK FAILED] Sum: " << sum << ", expected value: " << expected_sum << ", min: " << m_keys->at(pos_min) << ", max: " << m_keys->at(pos_max));
        }
    }

    virtual void run() override {
        std::mt19937_64 random_generator(seed);
        size_t length = static_cast<int64_t>(interval_sz * N);
        if(length < 1) length = 1;

//        std::cout << "interval_size: " << interval_sz << ", length: " << length << "\n";
        int64_t rstart = 0;
        int64_t rend = N - length;
        if(rend < rstart) { rend = rstart; length = N - rstart; } // [1, 1]
        uniform_int_distribution<size_t> distribution(rstart, rend);

        for(size_t i = 0; i < numLookups; i++){
            size_t outcome = distribution(random_generator);
            auto pos_min = outcome;
            auto pos_max = outcome + length -1;
            auto min = m_keys->at(pos_min);
            auto max = m_keys->at(pos_max);
            auto sum = pma->sum(min, max);
//            auto it_ptr = pma->find(min, max); // scoped ptr
//            auto it = it_ptr.get();
//            while(it->hasNext()){
//                sum += it->next().first;
//            }
            validate(sum, pos_min, pos_max);
        }
    }

};

} // anonymous namespace


ExperimentRangeQueryIntervals::ExperimentRangeQueryIntervals(std::shared_ptr<Interface> pmae, size_t N_inserts, size_t N_lookups, const vector<double>& intervals_)
    : pma_ptr(pmae), N_inserts(N_inserts), N_lookups(N_lookups), intervals(intervals_) {

    // before executing this experiment, check that the underlying data structure supports range queries
    pma_ptr->sum(0, 1);

    // default set of intervals
    if(intervals.empty()){
        for(double v = 0.001; v < 0.01; v += 0.001) intervals.push_back(v); // 0.1% --> 0.9%
        for(double v = 0.01; v <= 0.09; v += 0.01) intervals.push_back(v); // 1% --> 9%
        for(double v = 0.1; v <= 1; v += 0.1) intervals.push_back(v); // 10% -> 20% -> ... -> 90% -> 100%
    }
}

ExperimentRangeQueryIntervals::~ExperimentRangeQueryIntervals(){
    if(thread_pinned) unpin_thread();
}

void ExperimentRangeQueryIntervals::preprocess() {
    auto cpu_wanted = sched_getcpu();
    auto pma = pma_ptr.get();

    // Generate & insert the elements in the PMA
    assert(pma->size() == 0);
    LOG_VERBOSE("# Generating the elements ("<< N_inserts << ") ...");
    std::unique_ptr<Distribution> distribution{ generate_distribution() };
    bool distribution_dense = distribution->is_dense();

//    std::unique_ptr<RandomPermutationParallel> random_permutation_ptr(new RandomPermutationParallel());
//    auto random_permutation = random_permutation_ptr.get();
//    random_permutation->compute(N_inserts);
    pin_thread_to_cpu(cpu_wanted);
    thread_pinned = true;
    LOG_VERBOSE("# Inserting the elements (" << N_inserts << ") ...");
    Timer timer_inserts(true);
    for(size_t i = 0; i < N_inserts; i++){
        auto pair = distribution->get(i);
        pma->insert(pair.first, pair.second);
    }
    timer_inserts.stop();
    LOG_VERBOSE("# Insert time: " << timer_inserts.milliseconds() << " millisecs");

    // `Build' time
    Timer timer_build(true);
    pma->build();
    timer_build.stop();
    if(timer_build.milliseconds() > 0){
        LOG_VERBOSE("# Build time: " << timer_build.milliseconds() << " millisecs");
    }

    // Container for the keys
    Timer timer_container(true);
    if(distribution_dense){
        int64_t min = 0;
        int64_t length = pma->size();
        auto it = pma->iterator();
        if(it->hasNext()){ min = it->next().first; }

        m_keys.reset(new ContainerKeysDense(min, length));
    } else {
        m_keys.reset(new ContainerKeysSparse(pma));
    }
    timer_container.stop();
    if(timer_container.milliseconds() > 0){
        LOG_VERBOSE("# Key mapping time: " << timer_container.milliseconds() << " millisecs");
    }
}

void ExperimentRangeQueryIntervals::run() {
    auto pma = pma_ptr.get();
    assert(pma->size() == N_inserts);

    for(double interval : intervals){
        size_t num_lookups = interval < 0.1 ? N_lookups : N_lookups / 8;
        std::unique_ptr<ExperimentRangeQueryImpl> range_query(new
                ExperimentRangeQueryImpl(pma, interval, num_lookups, N_inserts, ARGREF(uint64_t, "seed_lookups"), m_keys.get()));
        ONLY_IF_PROFILING_ENABLED( BranchMispredictionsProfiler profiler; profiler.start() );
        range_query->execute();
        ONLY_IF_PROFILING_ENABLED( auto profiler_snapshot = profiler.stop() );

        REPORT_TIME_PROFILED("Interval: " << interval << ", lookups: " << num_lookups << ", elapsed time:", range_query->elapsed_millisecs() );
        config().db()->add("range_query")
                        ("interval", interval)
                        ("time", range_query->elapsed_millisecs())
                        ("num_lookups", num_lookups)
                        ;

        ONLY_IF_PROFILING_ENABLED(
            config().db()->add("profiler_range_query_brmsp")
                ("interval", interval)
                ("time", range_query->elapsed_millisecs())
                ("num_lookups", num_lookups)
                ("conditional_branches", profiler_snapshot.m_conditional_branches)
                ("branch_mispredictions", profiler_snapshot.m_branch_mispredictions)
                ("l1_misses", profiler_snapshot.m_cache_l1_misses)
                ("llc_misses", profiler_snapshot.m_cache_llc_misses);
        );
    }
}

} // namespace pma

