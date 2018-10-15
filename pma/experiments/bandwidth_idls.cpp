/*
 * bandwidth_idls.cpp
 *
 *  Created on: 24 Sep 2018
 *      Author: Dean De Leo
 */

#include "bandwidth_idls.hpp"

#include <cassert>
#include <cstring> // memset
#include <iostream>
#include <signal.h>
#include <unistd.h> // alarm

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

volatile static size_t TICK = 0; // periodically changed by the alarm signal handler
static void handle_alarm_signal(int signal){
    TICK++;
    alarm(1); // reset the alarm
} // signal handler

ExperimentBandwidthIDLS::ExperimentBandwidthIDLS(std::shared_ptr<Interface> pmae, size_t N_initial_inserts, size_t N_insdel, size_t N_consecutive_operations,
        std::string insert_distribution, double insert_alpha,
        std::string delete_distribution, double delete_alpha,
        double beta, uint64_t seed) :
    m_pma(pmae),
    N_initial_inserts(N_initial_inserts), N_insdel(N_insdel), N_consecutive_operations(N_consecutive_operations),
    m_distribution_type_insert(ExperimentIDLS::get_distribution_type(insert_distribution)), m_distribution_param_alpha_insert(insert_alpha),
    m_distribution_type_delete(ExperimentIDLS::get_distribution_type(delete_distribution)), m_distribution_param_alpha_delete(delete_alpha),
    m_distribution_param_beta(beta), m_distribution_seed(seed){

    if(beta <= 1){
        RAISE("Invalid value for the parameter --beta: " << beta << ". It defines the range of the distribution and it must be > 1");
    }

    // check the given PMA supports both inserts and deletes
    if(m_pma->size() != 0) { RAISE("The given PMA is not empty"); }
    m_pma->insert(1, 1);
    if(m_pma->size() != 1) { RAISE("Insertion failure. The PMA is still empty!"); }
    m_pma->remove(1);
    if(m_pma->size() != 0) { RAISE("Deletion failure. The PMA is not empty!"); }

    m_updates_per_second.reserve(60*60*24); // enough to run a single day
    m_updates_per_second.assign(m_updates_per_second.capacity(), 0);

    // install the signal handler
    struct sigaction signal_action;
    memset(&signal_action, 0, sizeof(signal_action));
    signal_action.sa_handler = handle_alarm_signal;
    int rc = sigaction(SIGALRM, &signal_action, nullptr);
    if(rc != 0){ RAISE("Cannot set the signal handler: " << strerror(errno) << " (" << errno << ")"); }
}

ExperimentBandwidthIDLS::~ExperimentBandwidthIDLS() {
    unpin_thread();
    sigaction(SIGALRM, nullptr, nullptr); // ignore return code
}

void ExperimentBandwidthIDLS::preprocess () {
    distribution::idls::Generator generator;
    generator.set_initial_size(N_initial_inserts);
    generator.set_insdel(N_insdel, N_consecutive_operations);
    generator.set_lookups(0);

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
}

void ExperimentBandwidthIDLS::run_updates(distribution::idls::Distribution<long>* distribution){
    auto pma = m_pma.get();

    while(distribution->hasNext()){
        auto key = distribution->next();
        if(key >= 0){ // insertion
            pma->insert(key, key);
        } else { // deletion
            key = -key;
#if !defined(NDEBUG)
            auto value = pma->remove(key);
            assert(value == key && "Key/value mismatch");
#else
            pma->remove(key);
#endif
        }
        assert(m_updates_per_second.size() > 0);
        m_updates_per_second[TICK]++;
    }
}

void ExperimentBandwidthIDLS::run_initial_inserts(){
    auto ptr = m_keys_experiment.preparation_step();
    auto distribution = ptr.get();

    TICK = 0;
    alarm(/* seconds */ 1);

    run_updates(distribution);
    assert(m_pma->size() == N_initial_inserts);

    alarm(0); // stop
}

void ExperimentBandwidthIDLS::run_insert_deletions(){
    auto ptr = m_keys_experiment.insdel_step();
    auto distribution = ptr.get();

    m_updates_per_second.clear();
    m_updates_per_second.assign(m_updates_per_second.capacity(), 0);
    TICK = 0;
    alarm(/* seconds */ 1);

    run_updates(distribution);
    assert(m_pma->size() == N_initial_inserts);

    alarm(0); // stop
}

void ExperimentBandwidthIDLS::run() {
    // Perform the initial inserts
    barrier(); Timer t_initial_inserts{true}; barrier();
    run_initial_inserts();
    barrier(); t_initial_inserts.stop(); barrier();
    m_keys_experiment.unset_preparation_step(); // release some memory

    REPORT_TIME("Initial step: " << N_initial_inserts << " insertions. Elapsed time:", t_initial_inserts.milliseconds());
    LOG_VERBOSE("Saving the insertion results into the database...");
    // this is going to take a while ...
    if(TICK > 0) TICK--;
    for(size_t i = 0, sz = TICK; i <= sz; i++){
        config().db()->add("bandwidth_idls")
                ("step", "insert_only")
                ("time", i)
                ("updates", m_updates_per_second[i])
                ;
    }

    barrier(); Timer t_updates{true}; barrier();
    run_insert_deletions();
    barrier(); t_updates.stop(); barrier();
    REPORT_TIME("IDLS step: " << N_insdel << " updates. Elapsed time:", t_updates.milliseconds());
    LOG_VERBOSE("Saving the update results into the database...");
    // this is going to take a while ...
    if(TICK > 0) TICK--;
    for(size_t i = 0, sz = TICK; i <= sz; i++){
        config().db()->add("bandwidth_idls")
                ("step", "update")
                ("time", i)
                ("updates", m_updates_per_second[i])
                ;
    }
}

} /* namespace pma */
