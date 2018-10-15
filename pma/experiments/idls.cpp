/*
 * idls.cpp
 *
 *  Created on: 5 Feb 2018
 *      Author: Dean De Leo
 */

#include "idls.hpp"

#include <cassert>
#include <iostream>

#include "configuration.hpp"
#include "database.hpp"
#include "miscellaneous.hpp" // pin_thread_to_cpu(), unpin_thread()
#include "pma/interface.hpp"

using namespace std;

// Error
#define RAISE(message) RAISE_EXCEPTION(pma::ExperimentError, message)

// Report the given message `preamble' together with the associated `time' in milliseconds
#define REPORT_TIME( preamble, time ) { std::cout << preamble << ' '; \
    if( time > 3000 ) { std::cout << (static_cast<double>(time) / 1000) << " seconds" << std::endl; } \
    else { std::cout << time << " milliseconds" << std::endl; } }

namespace pma {

ExperimentIDLS::ExperimentIDLS(std::shared_ptr<Interface> pmae, size_t N_initial_inserts, size_t N_insdel, size_t N_consecutive_operations,
    size_t N_lookups, size_t N_scans, const std::vector<double>& rq_intervals,
    std::string insert_distribution, double insert_alpha,
    std::string delete_distribution, double delete_alpha,
    double beta, uint64_t seed) :
    m_pma(pmae),
    N_initial_inserts(N_initial_inserts), N_insdel(N_insdel), N_consecutive_operations(N_consecutive_operations),
    N_lookups(N_lookups), N_scans(N_scans), m_range_query_intervals(rq_intervals),
    m_distribution_type_insert(get_distribution_type(insert_distribution)), m_distribution_param_alpha_insert(insert_alpha),
    m_distribution_type_delete(get_distribution_type(delete_distribution)), m_distribution_param_alpha_delete(delete_alpha),
    m_distribution_param_beta(beta), m_distribution_seed(seed) {

    if(beta <= 1){
        RAISE("Invalid value for the parameter --beta: " << beta << ". It defines the range of the distribution and it must be > 1");
    }

    // default set of intervals
    if(m_range_query_intervals.empty()){
        for(double v = 0.001; v < 0.01; v += 0.001) m_range_query_intervals.push_back(v); // 0.1% --> 0.9%
        for(double v = 0.01; v <= 0.09; v += 0.01) m_range_query_intervals.push_back(v); // 1% --> 9%
        for(double v = 0.1; v <= 1; v += 0.1) m_range_query_intervals.push_back(v); // 10% -> 20% -> ... -> 90% -> 100%
    }

    // check the given PMA supports both inserts and deletes
    if(m_pma->size() != 0) { RAISE("The given PMA is not empty"); }
    m_pma->insert(1, 1);
    if(m_pma->size() != 1) { RAISE("Insertion failure. The PMA is still empty!"); }
    m_pma->remove(1);
    if(m_pma->size() != 0) { RAISE("Deletion failure. The PMA is not empty!"); }
}

distribution::idls::eDistributionType ExperimentIDLS::get_distribution_type(string value) {
    if(value == "uniform"){
        return distribution::idls::eDistributionType::uniform;
    } else if (value == "zipf"){
        return distribution::idls::eDistributionType::zipf;
    } else if (value == "sequential" || value == "apma_sequential"){
        return distribution::idls::eDistributionType::sequential;
    } else {
        RAISE("Invalid distribution type: `" << value << "'");
    }
}

ExperimentIDLS::~ExperimentIDLS() { }

void ExperimentIDLS::preprocess () {
    distribution::idls::Generator generator;
    generator.set_initial_size(N_initial_inserts);
    generator.set_insdel(N_insdel, N_consecutive_operations);
    generator.set_lookups(N_lookups);

    // scans
    if(N_scans > 0){
        vector<pair<double, size_t>> scans;
        scans.reserve(m_range_query_intervals.size());
        for(size_t i = 0; i < m_range_query_intervals.size(); i++){
            double interval = m_range_query_intervals[i];
            size_t num_scans = interval < 0.1 ? N_scans : N_scans / 8;
            scans.emplace_back(interval, num_scans);
        }
        generator.set_scans(scans);
    }

    // distribution
    if(m_distribution_type_insert == distribution::idls::eDistributionType::sequential){
        generator.set_distribution_type_init(distribution::idls::eDistributionType::sequential, 1);
        generator.set_distribution_type_insert(distribution::idls::eDistributionType::sequential, N_initial_inserts);
    } else {
        generator.set_distribution_type_init(distribution::idls::eDistributionType::uniform, 0);
        generator.set_distribution_type_insert(m_distribution_type_insert, m_distribution_param_alpha_insert);
    }
    generator.set_distribution_type_delete(m_distribution_type_delete, m_distribution_param_alpha_delete);
    generator.set_distribution_range(m_distribution_param_beta);
    generator.set_seed(m_distribution_seed);

    // 8/5/2018
    // The IDLS distribution runs an (a,b)-tree to check and yields the keys to insert & delete, so that we are guaranteed that those
    // keys exist when the experiment runs. Furthermore, additional vectors are built, to store the generated keys. To avoid additional
    // overhead when running the actual experiment, the idea is to store all these side data structures in the secondary memory node (node=1),
    // while running the experiment on the first numa node (node=0).

    // if NUMA, make the keys in the secondary socket
    int numa_max_node = get_numa_max_node();
    if(numa_max_node >= 1){
        LOG_VERBOSE("Pinning to NUMA node 1");
        pin_thread_to_numa_node(1);
    }

    // generate the keys to use in the experiment
    LOG_VERBOSE("Generating the keys for the experiment...");
    m_keys_experiment = generator.generate();

    // now move to the first socket
    if(numa_max_node >= 1){
        LOG_VERBOSE("Pinning to NUMA node 0");
        pin_thread_to_numa_node(0);
    }

    // pin the current thread
    pin_thread_to_cpu();
    m_thread_pinned = true;
}



void ExperimentIDLS::run_initial_inserts(){
    Interface* pma = m_pma.get();
    auto prep_step_ptr = m_keys_experiment.preparation_step();
    auto distribution = prep_step_ptr.get();

    for(size_t i =0, sz = N_initial_inserts; i < sz; i++){
        assert(distribution->hasNext() && "Expected `N_initial_inserts' keys");
        auto key = distribution->next();
        pma->insert(key, key);
    }

    assert(pma->size() == N_initial_inserts);
}

void ExperimentIDLS::run_inserts(distribution::idls::Distribution<int64_t>* __restrict distribution, size_t count){
    assert(distribution != nullptr);
    Interface* __restrict pma = m_pma.get();

    for(size_t i = 0; i < count; i++){
        assert(distribution->hasNext() && "Expected `count' keys");
        auto key = distribution->next();
        assert(key > 0 && "Expected a positive value (otherwise it's a deletion!).");
        pma->insert(key, key);
    }
}

void ExperimentIDLS::run_deletions(distribution::idls::Distribution<int64_t>* __restrict distribution, size_t count){
    Interface* __restrict pma = m_pma.get();

    for(size_t i = 0; i < count; i++){
        assert(distribution->hasNext() && "Expected `count' keys");
        auto key = - (distribution->next());
        assert(key > 0 && "Expected a negative value (otherwise it's an insertion!).");

#if !defined(NDEBUG)
        auto value = pma->remove(key);
        assert(value == key && "Key/value mismatch");
#else
        pma->remove(key);
#endif
    }
}

void ExperimentIDLS::run_lookups(){
    Interface* __restrict pma = m_pma.get();
    auto lookup_step_ptr = m_keys_experiment.lookup_step();
    auto distribution = lookup_step_ptr.get();

    for(size_t i = 0; i < N_lookups; i++){
        assert(distribution->hasNext() && "Expected `N_lookups' keys");
        auto key = distribution->next();
        assert(key > 0 && "Expected a positive value for the key");

#if !defined(NDEBUG)
        auto value = pma->find(key);
        assert(value == key && "Key/value mismatch");
#else
        pma->find(key);
#endif
    }
}

void ExperimentIDLS::run_scans(distribution::idls::Distribution<distribution::idls::ScanRange>* distribution, size_t count){
    assert(distribution != nullptr);
    Interface* __restrict pma = m_pma.get();

    for(size_t i = 0; i < count; i++){
        assert(distribution->hasNext() && "Expected `count' scans");
        auto scan_range = distribution->next();

        // range scan in [key_min, key_max]
        auto sum = pma->sum(scan_range.key_min, scan_range.key_max);

        // validate
        if(sum.m_sum_keys != scan_range.expected_sum_keys || sum.m_sum_values != scan_range.expected_sum_values){
            RAISE("[SANITY CHECK FAILED] Sum: " << sum << ", expected values, "
                    "keys: " << scan_range.expected_sum_keys << ", "
                    "values: " << scan_range.expected_sum_values << ", "
                    "min: " << scan_range.key_min << ", max: " << scan_range.key_max );
        }
    }
}

void ExperimentIDLS::run(){
    // Perform the initial inserts
    Timer t_initial_inserts{true};
    run_initial_inserts();
    t_initial_inserts.stop();
    m_keys_experiment.unset_preparation_step(); // release some memory
    REPORT_TIME("Initial step: " << N_initial_inserts << " insertions. Elapsed time:", t_initial_inserts.milliseconds());

    int num_resizes = 0;
    uint64_t previous_memory_footprint = m_pma->memory_footprint();

    // Sequence of insert/deletes
    Timer t_insert; size_t count_insertions = 0;
    Timer t_delete; size_t count_deletions = 0;
    { // restrict the scope
        size_t count = 0;
        auto ptr = m_keys_experiment.insdel_step();
        auto distribution = ptr.get();
        int64_t size0 = m_pma->size();

        while(count < N_insdel){
            // Insertions
            t_insert.start();
            run_inserts(distribution, N_consecutive_operations);
            t_insert.stop();
            count_insertions += N_consecutive_operations;

            auto memory_footprint = m_pma->memory_footprint();
            if(memory_footprint != previous_memory_footprint){
                num_resizes++; previous_memory_footprint = memory_footprint;
            }

            // Deletions
            t_delete.start();
            run_deletions(distribution, N_consecutive_operations);
            t_delete.stop();
            count_deletions += N_consecutive_operations;

            memory_footprint = m_pma->memory_footprint();
            if(memory_footprint != previous_memory_footprint){
                num_resizes++; previous_memory_footprint = memory_footprint;
            }

            count += 2* N_consecutive_operations;
        }

        int64_t size1 = m_pma->size();
        int64_t diff = size1 - size0;
        if(diff <= 0){ // bring back to its initial size
            run_inserts(distribution, -diff);
        } else {
            run_deletions(distribution, diff);
        }

        if(m_pma->memory_footprint() != previous_memory_footprint){
            num_resizes++; previous_memory_footprint = m_pma->memory_footprint();
        }

        // release some additional memory
        m_keys_experiment.unset_insdel_step();
    }
    REPORT_TIME("Additional insertions: " << count_insertions << " in sequences of " << N_consecutive_operations << " operations. Elapsed time:", t_insert.milliseconds());
    REPORT_TIME("Additional deletions: " << count_deletions << " in sequences of " << N_consecutive_operations << " operations. Elapsed time:", t_delete.milliseconds());

    // Lookups
    Timer t_lookups;
    if(N_lookups > 0){
        t_lookups.start();
        run_lookups();
        t_lookups.stop();
        REPORT_TIME("Lookups: " << N_lookups << ". Elapsed time:", t_lookups.milliseconds());
    }

    LOG_VERBOSE("Counted resizes: " << num_resizes << ", memory_footprint: " << previous_memory_footprint);
    config().db()->add("idls_main")
                    ("initial_size", N_initial_inserts)
                    ("time_initial_size", t_initial_inserts.milliseconds<uint64_t>())
                    ("inserts", count_insertions)
                    ("t_inserts", t_insert.milliseconds<uint64_t>())
                    ("deletes", count_deletions)
                    ("t_deletes", t_delete.milliseconds<uint64_t>())
                    ("lookups", N_lookups)
                    ("t_lookups", t_lookups.milliseconds<uint64_t>())
                    ("num_resizes", num_resizes)
                    ("memory_footprint", previous_memory_footprint)
                    ;

    // Range queries
    const auto& scans = m_keys_experiment.scan_step();
    for(auto& pair : scans){
        double interval = pair.first;
        auto& ptr = pair.second;
        auto distribution = ptr.get();
        size_t num_repetitions = interval < 0.1 ? N_scans : N_scans / 8;

        Timer t_scan(true);
        run_scans(distribution, num_repetitions);
        t_scan.stop();

        REPORT_TIME("Interval: " << interval << ", scans: " << num_repetitions << ", elapsed time:", t_scan.milliseconds() );
        config().db()->add("idls_range_query")
                        ("interval", interval)
                        ("time", t_scan.milliseconds<uint64_t>())
                        ("num_scans", num_repetitions)
                        ;
    }
}

void ExperimentIDLS::postprocess(){
    if(m_thread_pinned){
        unpin_thread();
    }
}

} /* namespace pma */
