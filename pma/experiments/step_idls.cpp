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

#include "step_idls.hpp"

#include <cassert>
#include <cstring> // memset
#include <iostream>

#include "configuration.hpp"
#include "database.hpp"
#include "distribution/distribution.hpp"
#include "errorhandling.hpp"
#include "idls.hpp"
#include "miscellaneous.hpp" // pin_thread_to_cpu(), unpin_thread()
#include "pma/interface.hpp"
#include "timer.hpp"

using namespace std;
// Error
#define RAISE(message) RAISE_EXCEPTION(pma::ExperimentError, message)
// Report the given message `preamble' together with the associated `time' in milliseconds
#define REPORT_TIME( preamble, time ) { std::cout << preamble << ' '; \
    if( time > 3000 ) { std::cout << (static_cast<double>(time) / 1000) << " seconds" << std::endl; } \
    else { std::cout << time << " milliseconds" << std::endl; } }

namespace pma {

ExperimentStepIDLS::ExperimentStepIDLS(std::shared_ptr<Interface> pma, size_t initial_size, size_t final_size, size_t step_size, size_t num_scans,
        std::string insert_distribution, double insert_alpha,
        std::string delete_distribution, double delete_alpha,
        double beta, uint64_t seed) :
            m_interface(pma),
            m_initial_size(initial_size), m_final_size(final_size), m_step_size(step_size), m_num_scans(num_scans),
            m_distribution_type_insert(ExperimentIDLS::get_distribution_type(insert_distribution)), m_distribution_param_alpha_insert(insert_alpha),
            m_distribution_type_delete(ExperimentIDLS::get_distribution_type(delete_distribution)), m_distribution_param_alpha_delete(delete_alpha),
            m_distribution_param_beta(beta), m_distribution_seed(seed) {
    if(pma.get() == nullptr) RAISE("The pointer data structure is NULL");
    if(initial_size > final_size) RAISE("Initial size is greater than final size: " << initial_size << " <= " << final_size);
    if(step_size == 0) RAISE("step size is 0");

    if(beta <= 1){
        RAISE("Invalid value for the parameter --beta: " << beta << ". It defines the range of the distribution and it must be > 1");
    }

    // check the given PMA supports both inserts and deletes
    if(m_interface->size() != 0) { RAISE("The given PMA is not empty"); }
    m_interface->insert(1, 1);
    if(m_interface->size() != 1) { RAISE("Insertion failure. The PMA is still empty!"); }
    m_interface->remove(1);
    if(m_interface->size() != 0) { RAISE("Deletion failure. The PMA is not empty!"); }
}

ExperimentStepIDLS::~ExperimentStepIDLS() {
    if(m_thread_pinned){ unpin_thread(); }
}

void ExperimentStepIDLS::preprocess () {
    distribution::idls::Generator generator;
    generator.set_initial_size(m_initial_size);
    size_t group_size = m_final_size - m_initial_size;
    generator.set_insdel(group_size * 2, group_size);
    generator.set_lookups(0);

    // distribution
    if(m_distribution_type_insert == distribution::idls::eDistributionType::sequential){
        generator.set_distribution_type_init(distribution::idls::eDistributionType::sequential, 1);
        generator.set_distribution_type_insert(distribution::idls::eDistributionType::sequential, m_initial_size);
    } else {
        generator.set_distribution_type_init(distribution::idls::eDistributionType::uniform, 0);
        generator.set_distribution_type_insert(m_distribution_type_insert, m_distribution_param_alpha_insert);
    }
    generator.set_distribution_type_delete(m_distribution_type_delete, m_distribution_param_alpha_delete);
    generator.set_distribution_range(m_distribution_param_beta);
    generator.set_seed(m_distribution_seed);
    generator.set_restore_initial_size(true);

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
        m_thread_pinned = true;
    }

    // initial size
    if(m_initial_size > 0){
        auto distribution_ptr = m_keys_experiment.preparation_step();
        Timer timer;
        LOG_VERBOSE("Inserting " << m_initial_size << " to reach the initial size of the data structure...");
        timer.start();
        run_updates<true>(distribution_ptr.get());
        timer.stop();
        LOG_VERBOSE("# Insertion time (initial size): " << timer.milliseconds() << " millisecs");

        m_keys_experiment.unset_preparation_step(); // release some memory
    }
}

/**
 * Perform `num_scan' scans into the given pma.
 */
static void do_scans(Interface* pma, uint64_t num_scans){
    assert(pma->size() > 0);

    for(size_t i = 0; i < num_scans; i++){
        pma->sum(numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());
    }
}

template<bool do_inserts>
void ExperimentStepIDLS::run_updates(distribution::idls::Distribution<long>* distribution, int64_t max_step){
    auto interface = m_interface.get();

    int64_t i = 0;
    while(i < max_step && distribution->hasNext()){
        auto key = distribution->next();
        if(do_inserts){
            assert(key >= 0 && "Expected a positive number for insertions");
            interface->insert(key, key);
        } else {
            assert(key < 0 && "Expected a negative number for deletions");
            key = -key; // flip the sign
#if !defined(NDEBUG)
            auto value = interface->remove(key);
            assert(value == key && "Key/value mismatch");
#else
            interface->remove(key);
#endif
        }

        i++;
    }
}
//
//void ExperimentBandwidthIDLS::run_initial_inserts(){
//    auto ptr = m_keys_experiment.preparation_step();
//    auto distribution = ptr.get();
//
//    TICK = 0;
//    alarm(/* seconds */ 1);
//
//    run_updates(distribution);
//    assert(m_pma->size() == N_initial_inserts);
//
//    alarm(0); // stop
//}
//
//void ExperimentBandwidthIDLS::run_insert_deletions(){
//    auto ptr = m_keys_experiment.insdel_step();
//    auto distribution = ptr.get();
//
//    m_updates_per_second.clear();
//    m_updates_per_second.assign(m_updates_per_second.capacity(), 0);
//    TICK = 0;
//    alarm(/* seconds */ 1);
//
//    run_updates(distribution);
//    assert(m_pma->size() == N_initial_inserts);
//
//    alarm(0); // stop
//}
//
//void ExperimentBandwidthIDLS::run() {
//    // Perform the initial inserts
//    barrier(); Timer t_initial_inserts{true}; barrier();
//    run_initial_inserts();
//    barrier(); t_initial_inserts.stop(); barrier();
//    m_keys_experiment.unset_preparation_step(); // release some memory
//
//    REPORT_TIME("Initial step: " << N_initial_inserts << " insertions. Elapsed time:", t_initial_inserts.milliseconds());
//    LOG_VERBOSE("Saving the insertion results into the database...");
//    // this is going to take a while ...
//    if(TICK > 0) TICK--;
//    for(size_t i = 0, sz = TICK; i <= sz; i++){
//        config().db()->add("bandwidth_idls")
//                ("step", "insert_only")
//                ("time", i)
//                ("updates", m_updates_per_second[i])
//                ;
//    }
//
//    barrier(); Timer t_updates{true}; barrier();
//    run_insert_deletions();
//    barrier(); t_updates.stop(); barrier();
//    REPORT_TIME("IDLS step: " << N_insdel << " updates. Elapsed time:", t_updates.milliseconds());
//    LOG_VERBOSE("Saving the update results into the database...");
//    // this is going to take a while ...
//    if(TICK > 0) TICK--;
//    for(size_t i = 0, sz = TICK; i <= sz; i++){
//        config().db()->add("bandwidth_idls")
//                ("step", "update")
//                ("time", i)
//                ("updates", m_updates_per_second[i])
//                ;
//    }
//}

void ExperimentStepIDLS::run() {
    size_t current_size = m_initial_size;
    Interface* pma = m_interface.get();
    Timer timer_insert, timer_delete;
    uniform_int_distribution<int64_t> random_lookups(0, std::numeric_limits<int64_t>::max());
    size_t memory_footprint = pma->memory_footprint();

    auto distribution_ptr = m_keys_experiment.insdel_step();
    auto distribution = distribution_ptr.get();

    // insertion phase
    while(current_size < m_final_size){
        if(pma->size() > 0 && m_num_scans > 0){
            cout << "[" << pma->size() << "] Perfoming " << m_num_scans << " scans ..." << endl;
            Timer timer_scan;
            timer_scan.start();
            do_scans(pma, m_num_scans);
            timer_scan.stop();
            uint64_t t_scan = timer_scan.milliseconds();

            cout << "[" << pma->size() << "] # Scan time (total): " << t_scan << " milliseconds" << endl;

            // save the result
            config().db()->add("step_idls")
                            ("type", "scan_insert")
                            ("initial_size", current_size)
                            ("elements", m_num_scans)
                            ("time", t_scan)
                            ("space_usage", memory_footprint);
        }


        // Insert `step' elements for the next iteration
        size_t step_size = current_size + m_step_size > m_final_size ? m_final_size - current_size : m_step_size;
        size_t next_size = current_size + step_size;
        if(step_size > 0){
            cout << "[" << m_interface->size() << "] Inserting " << step_size << " elements ..." << endl;
            timer_insert.start();
            run_updates<true>(distribution, step_size);
            timer_insert.stop();
            uint64_t t_insert = timer_insert.milliseconds();
            cout << "[" << m_interface->size() << "] # Insertion time (total): " << t_insert << " milliseconds" <<endl;

            pma->build();
            memory_footprint = pma->memory_footprint();

            assert(pma->size() == next_size);

            // save the result
            config().db()->add("step_idls")
                            ("type", "insert")
                            ("initial_size", current_size)
                            ("elements", next_size)
                            ("time", t_insert)
                            ("space_usage", memory_footprint);

            current_size = next_size;
        }
    }

    // last set of scans for insertions
    if(pma->size() > 0 && m_num_scans > 0){
        cout << "[" << pma->size() << "] Perfoming " << m_num_scans << " scans ..." << endl;
        Timer timer_scan;
        timer_scan.start();
        do_scans(pma, m_num_scans);
        timer_scan.stop();
        uint64_t t_scan = timer_scan.milliseconds();

        cout << "[" << pma->size() << "] # Scan time (total): " << t_scan << " milliseconds" << endl;

        // save the result
        config().db()->add("step_idls")
                        ("type", "scan_insert")
                        ("initial_size", current_size)
                        ("elements", m_num_scans)
                        ("time", t_scan)
                        ("space_usage", memory_footprint);
    }

    // deletion phase
    while(current_size > m_initial_size){
        if(pma->size() > 0 && m_num_scans > 0){
            cout << "[" << pma->size() << "] Perfoming " << m_num_scans << " scans ..." << endl;
            Timer timer_scan;
            timer_scan.start();
            do_scans(pma, m_num_scans);
            timer_scan.stop();
            uint64_t t_scan = timer_scan.milliseconds();

            cout << "[" << pma->size() << "] # Scan time (total): " << t_scan << " milliseconds" << endl;

            // save the result
            config().db()->add("step_idls")
                            ("type", "scan_delete")
                            ("initial_size", current_size)
                            ("elements", m_num_scans)
                            ("time", t_scan)
                            ("space_usage", memory_footprint);
        }


        // Delete `step' elements for the next iteration
        size_t step_size = current_size < m_initial_size + m_step_size ? current_size - m_initial_size : m_step_size;
        size_t next_size = current_size - step_size;
        if(step_size > 0){
            cout << "[" << m_interface->size() << "] Removing " << step_size << " elements ..." << endl;
            timer_delete.start();
            run_updates<false>(distribution, step_size);
            timer_delete.stop();
            uint64_t t_delete = timer_delete.milliseconds();
            cout << "[" << m_interface->size() << "] # Deletion time (total): " << t_delete << " milliseconds" <<endl;

            pma->build();
            memory_footprint = pma->memory_footprint();

            assert(pma->size() == next_size);

            // save the result
            config().db()->add("step_idls")
                            ("type", "delete")
                            ("initial_size", current_size)
                            ("elements", next_size)
                            ("time", t_delete)
                            ("space_usage", memory_footprint);

            current_size = next_size;
        }
    }

    // last set of scans for deletions
    if(pma->size() > 0 && m_num_scans > 0){
        cout << "[" << pma->size() << "] Perfoming " << m_num_scans << " scans ..." << endl;
        Timer timer_scan;
        timer_scan.start();
        do_scans(pma, m_num_scans);
        timer_scan.stop();
        uint64_t t_scan = timer_scan.milliseconds();

        cout << "[" << pma->size() << "] # Scan time (total): " << t_scan << " milliseconds" << endl;

        // save the result
        config().db()->add("step_idls")
                        ("type", "scan_delete")
                        ("initial_size", current_size)
                        ("elements", m_num_scans)
                        ("time", t_scan)
                        ("space_usage", memory_footprint);
    }
}

} /* namespace pma */
