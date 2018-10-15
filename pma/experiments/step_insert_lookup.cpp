/*
 * step_insert_lookup.cpp
 *
 *  Created on: 28 Dec 2017
 *      Author: Dean De Leo
 */

#include "step_insert_lookup.hpp"

#include <cassert>
#include <iostream>
#include <random>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "database.hpp"
#include "miscellaneous.hpp"
#include "timer.hpp"

#include "distribution/distribution.hpp"
#include "distribution/driver.hpp"
#include "pma/interface.hpp"

using namespace distribution;
using namespace std;

#define RAISE(message) RAISE_EXCEPTION(pma::ExperimentError, message)

namespace pma {

ExperimentStepInsertLookup::ExperimentStepInsertLookup(shared_ptr<Interface> pma, size_t N, size_t M) : interface(pma),
        N_inserts(N), N_lookups(M), distribution(nullptr){
    if(pma.get() == nullptr) RAISE("The pointer data structure is NULL");
    if(N_inserts == 0) RAISE("Invalid number of insertions: " << N_inserts);
}

ExperimentStepInsertLookup::~ExperimentStepInsertLookup(){
    if(thread_pinned){ unpin_thread(); }
}

void ExperimentStepInsertLookup::preprocess() {
    auto initial_size = ARGREF(int64_t, "initial_size");
    if(initial_size.is_set() && initial_size > 0){
        std::cout << "[ExperimentInsertLookup::preprocess] WARNING: parameter initial size ignored in this experiment\n";
    }

    // initialize the elements that we need to add
    LOG_VERBOSE("Generating the set of elements to insert ... ");
    distribution = generate_distribution();

    pin_thread_to_cpu();
    thread_pinned = true;
    LOG_VERBOSE("Experiment ready to begin");
}

/**
 * Insert all the elements in the permutation into the pma.
 */
static void step_insert(Interface* pma, Distribution* elements){
    for(size_t i = 0, elements_sz = elements->size(); i < elements_sz; i++){
        auto e = elements->get(i);
        pma->insert(e.first, e.second);
    }
}

/**
 * Perform `num_lookup' searches into the given pma. Use the first N elements of the permutation for the keys to search,
 * where N = pma->size();
 */
static void step_lookup(Interface* pma, Distribution* permutation, uint64_t num_lookups, uint64_t random_seed) {
    mt19937_64 random_generator(random_seed);
    uniform_int_distribution<int64_t> distribution(0, pma->size() == 0 ? 0 : pma->size() -1);

    for(size_t i = 0; i < num_lookups; i++){
        pma->find(permutation->get( distribution(random_generator) ).first +1 );
    }
}

void ExperimentStepInsertLookup::run() {
    auto pma = interface.get();
    size_t N = 0, step = 1024;
    uint64_t t_insert = 0;
    mt19937_64 random_generator(ARGREF(uint64_t, "seed_lookups"));
    uniform_int_distribution<int64_t> random_lookups(0, std::numeric_limits<int64_t>::max());
    Timer aux_timer;

    while(N + step <= N_inserts){

        { // insert `step elements'
            std::unique_ptr<Distribution> view{ distribution->view(N, step) };
            cout << "[" << interface->size() << "] Inserting " << step << " elements ..." << endl;
            aux_timer.reset(true);
            step_insert(pma, view.get());
            aux_timer.stop();
            t_insert += aux_timer.milliseconds();
            cout << "[" << interface->size() << "] # Insertion time (total): " << t_insert << endl;
            N += step;

            pma->build(); // in case of the Static-ABTree we build the tree only after calling #build()
            assert(interface->size() == N);

            // save the result
            config().db()->add("step_insert_lookup")
                            ("type", "insert")
                            ("initial_size", N) // this is confusing, it's actually the final size after the insertions
                            ("elements", step) // this is the number of elements inserted in the last step
                            ("time", t_insert); // this is the total time, i.e. to insert all elements in the data structure, not just the last step
        }


        // search the elements in the data structure
        if(N_lookups > 0){
            cout << "[" << interface->size() << "] Searching " << N_lookups << " elements ..." << endl;
            aux_timer.reset(true);
            step_lookup(pma, distribution.get(), N_lookups, random_lookups(random_generator) + 13);
            aux_timer.stop();
            uint64_t t_search = aux_timer.milliseconds();
            cout << "[" << interface->size() << "] # Search time (total): " << t_search << endl;

            // save the result
            config().db()->add("step_insert_lookup")
                            ("type", "search")
                            ("initial_size", N)
                            ("elements", N_lookups)
                            ("time", t_search);
        }

        step = N; // next step
    }
}

} // namespace pma
