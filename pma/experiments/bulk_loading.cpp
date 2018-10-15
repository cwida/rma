/*
 * bulk_loading.cpp
 *
 *  Created on: 18 May 2018
 *      Author: Dean De Leo
 */

#include "bulk_loading.hpp"

#include <cassert>
#include <iostream>
#include <limits>
#include <random>
#include <utility>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "database.hpp"
#include "errorhandling.hpp"
#include "distribution/cbyteview.hpp"
#include "distribution/driver.hpp"
#include "distribution/zipf_distribution.hpp"
#include "miscellaneous.hpp"
#include "pma/bulk_loading.hpp"
#include "pma/interface.hpp"

using namespace std;

#define RAISE(message) RAISE_EXCEPTION(pma::ExperimentError, message)
#define REPORT_TIME( message, timer ) { std::cout << message; \
    if( timer.milliseconds() > 3000 ) { std::cout << (static_cast<double>(timer.milliseconds()) / 1000) << " seconds" << std::endl; } \
    else { std::cout << timer.milliseconds() << " milliseconds" << std::endl; } }

namespace pma {

shared_ptr<BulkLoading> ExperimentBulkLoading::get_bulk_loading_interface(shared_ptr<Interface> interface){
    shared_ptr<BulkLoading> result = std::dynamic_pointer_cast<BulkLoading>(interface);
    if(!result) RAISE("The given data structure does not support bulk loading");
    return result;
}

uint64_t ExperimentBulkLoading::random_generator_seed(){
    uint64_t user_seed = ARGREF(uint64_t, "seed_random_permutation");
    return user_seed ^ 11364247648564936763ULL;
}

uint32_t ExperimentBulkLoading::random_generator_max(){
    uint32_t result = numeric_limits<int32_t>::max(); // int32_t, ignore the most significant bit
    auto param_beta = ARGREF(double, "beta");
    if(param_beta.is_set()){ result = param_beta.get(); }
    return result;
}

ExperimentBulkLoading::ExperimentBulkLoading(shared_ptr<Interface> interface, size_t initial_size, size_t batch_size, size_t num_batches, bool initial_size_uniform) :
        m_interface(interface), m_initial_size(initial_size), m_batch_size(batch_size), m_num_batches(num_batches), m_initial_size_uniform(initial_size_uniform){
    if(m_batch_size == 0) RAISE("Invalid value for batch size: " << m_batch_size);
    if(m_num_batches == 0) RAISE("Invalid value for `num_batches': " << m_num_batches);
    // side effect: check that the given PMA supports bulk loads
    if(m_batch_size > 1) get_bulk_loading_interface(m_interface);
    // check that the distribution is compatible with the uniform distribution
    if(m_initial_size_uniform && m_initial_size > 0){
        string distribution = ARGREF(string, "distribution");
        if(distribution == "uniform"){
            // the distribution is already uniform, there is no need to load the elements in a different way
            m_initial_size_uniform = false;
        } else if (distribution != "zipf"){
            RAISE("Only the `uniform' and `zipf' distributions are supported when elements should be loaded with a uniform distribution");
        }
    }
}

ExperimentBulkLoading::~ExperimentBulkLoading(){
    if(m_thread_pinned)
        unpin_thread();
}

void ExperimentBulkLoading::preload(){
    int64_t max = random_generator_max();
    mt19937_64 generator{ random_generator_seed() };
    uniform_int_distribution<int64_t> distribution{1, max};

    LOG_VERBOSE("Filling the data structure with `" << m_initial_size << "' elements, uniform distribution... ");
    Timer timer{true};
    for(size_t i = 0; i < m_initial_size; i++){
        int64_t key_high = distribution(generator);
        int64_t key_low = i;
        int64_t key = (key_high << 32) | key_low;
        m_interface->insert(key, key * 10);
    }
    timer.stop(); // do not record this time in the database, the preload operation is different with the uniform & zipf distributions
    REPORT_TIME("Initial load executed in: ", timer);
}

void ExperimentBulkLoading::preload(distribution::Distribution* distribution){
    assert(distribution != nullptr && "Null pointer for the argument `distribution'");
    LOG_VERBOSE("Filling the data structure with `" << m_initial_size << "' elements, user distribution... ");
    Timer timer{true};
    for(size_t i = 0; i < m_initial_size; i++){
        auto p = distribution->get(i);
        m_interface->insert(p.first, p.second);
    }
    timer.stop(); // do not record this time in the database, the preload operation is different with the uniform & zipf distributions
    REPORT_TIME("Initial load executed in: ", timer);
}

void ExperimentBulkLoading::preprocess(){
    if(m_initial_size > 0){ // preload the data structure

        if(m_initial_size_uniform){
            // if the user distribution is `zipf', we still preload the data structure with a uniform distribution
            string distribution_name = ARGREF(string, "distribution");
            if(distribution_name == "zipf"){
                double alpha = ARGREF(double, "alpha");
                if(alpha <= 0) RAISE("Invalid value for the parameter --alpha: " << alpha << ". It should be a value > 0");
                size_t num_outcomes = m_batch_size * m_num_batches;
                uint64_t seed = ARGREF(uint64_t, "seed_random_permutation");
                double beta = ARGREF(double, "beta");
                if(beta < 1) RAISE("Invalid value for the parameter --beta: " << beta << ". It should be a value >= 1");
                uint64_t range = beta;

                preload(); // preload the elements with a uniform distribution
                LOG_VERBOSE("Initialising the zipf distribution...");
                m_distribution = distribution::make_zipf(alpha, num_outcomes, range, seed, m_initial_size);
            } else {
                RAISE("Distribution not supported with distribution `" << distribution_name << "' when loading the first"
                        " `" << m_initial_size << "' elements with a uniform distribution");
            }
        } else {
            // initialize the elements that we need to add
            auto distribution_ptr = distribution::generate_distribution();
            preload(distribution_ptr.get());
            LOG_VERBOSE("Initialising the distribution...");
            m_distribution = distribution_ptr->view(m_initial_size);
        }

    } else {
        // initialise the distribution to load the batches
        m_distribution = distribution::generate_distribution();
    }


    // thread pinning
    pin_thread_to_cpu();
    m_thread_pinned = true;
    LOG_VERBOSE("Experiment ready to begin!");
}

void ExperimentBulkLoading::run(){
    // if the batch size is 1, then inserts all elements one by one using the traditional interface, that is pma::insert(key, value)
    if(m_batch_size == 1){
        run_insert();

    // otherwise insert the elements in batches, using the interface pma::load(batch, batch_sz);
    } else {
        run_load();
    }
}

void ExperimentBulkLoading::run_insert(){
    LOG_VERBOSE("Initial size: " << m_interface->size() << ", elements to insert: " << m_num_batches);
    assert(m_batch_size == 1);
    auto interface = m_interface.get();
    auto distribution = m_distribution.get();

    Timer timer {true};
    for(size_t i = 0; i < m_num_batches; i++){
        auto pair = distribution->get(i);
        interface->insert(pair.first, pair.second);
    }
    timer.stop();

    REPORT_TIME("Elements inserted in: ", timer);

    config().db()->add("bulk_loading")
            ("size", m_initial_size)
            ("time", timer.microseconds());
}

void ExperimentBulkLoading::run_load(){
    LOG_VERBOSE("Initial size: " << m_interface->size() << ", batch size: " << m_batch_size << ", number of batches: " << m_num_batches);
    Timer timer_batch, timer_total;

    unique_ptr<pair<int64_t, int64_t>[]> batch_ptr{ new pair<int64_t, int64_t>[m_batch_size] };
    auto batch = batch_ptr.get();
    shared_ptr<BulkLoading> interface_ptr = get_bulk_loading_interface(m_interface);
    auto interface = interface_ptr.get();
    assert(interface != nullptr && "The data structure does not support bulk loads");

    for(size_t i = 0; i < m_num_batches; i++){
        LOG_VERBOSE("Loading batch: " << (i+1) << "/" << m_num_batches);
        size_t initial_size = m_interface->size();

        // gather the elements to load
        auto distribution = m_distribution->view(i * m_batch_size, m_batch_size);
        for(size_t j = 0; j < m_batch_size; j++){
            batch[j] = distribution->get(j);
        }

        timer_total.start();
        timer_batch.reset(true);
        interface->load(batch, m_batch_size);
        timer_batch.stop();
        timer_total.stop();

#if !defined(NDEBUG)
        size_t final_size = m_interface->size();
        assert(final_size == initial_size + m_batch_size && "Some elements have not been loaded?");
#endif

        REPORT_TIME("Batch loaded in: ", timer_batch);

        config().db()->add("bulk_loading")
                ("size", initial_size)
                ("time", timer_batch.microseconds());
    }

    REPORT_TIME(m_num_batches << " batches loaded in: ", timer_total);
}



} // namespace pma



