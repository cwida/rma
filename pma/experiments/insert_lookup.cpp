/*
 * insert_lookup.cpp
 *
 *  Created on: 19 Jan 2018
 *      Author: Dean De Leo
 */

#include "insert_lookup.hpp"

#include <cassert>
#include <iostream>
#include <random>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "database.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp"
#include "timer.hpp"

#include "distribution/distribution.hpp"
#include "distribution/driver.hpp"
#include "pma/interface.hpp"

#define RAISE(message) RAISE_EXCEPTION(pma::ExperimentError, message)

using namespace distribution;
using namespace std;

namespace pma {

ExperimentInsertLookup::ExperimentInsertLookup(std::shared_ptr<Interface> pma, size_t N, size_t M) :
        interface(pma),  N_inserts(N), N_lookups(M), distribution(nullptr){
    if(pma.get() == nullptr) RAISE("The pointer data structure is NULL");
    if(N_inserts == 0) RAISE("Invalid number of insertions: " << N_inserts);
}

ExperimentInsertLookup::~ExperimentInsertLookup() {
    if(thread_pinned){ unpin_thread(); }
}

void ExperimentInsertLookup::preprocess() {
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

void ExperimentInsertLookup::do_inserts(Interface* pma, Distribution* distribution){
    for(size_t i = 0; i < N_inserts; i++){
        auto p = distribution->get(i);
        pma->insert(p.first, p.second);
    }
}

void ExperimentInsertLookup::do_lookups(Interface* pma, Distribution* permutation, size_t seed){
    mt19937_64 random_generator(seed);
    uniform_int_distribution<int64_t> distribution(0, pma->size() == 0 ? 0 : pma->size() -1);

    for(size_t i = 0; i < N_lookups; i++){
        pma->find(permutation->get( distribution(random_generator) ).first +1 );
    }
}

void ExperimentInsertLookup::run() {
    auto pma = interface.get();
    Timer aux_timer;

    cout << "Inserting " << N_inserts << " elements ..." << endl;
    aux_timer.reset(true);
    do_inserts(pma, distribution.get());
    aux_timer.stop();
    size_t t_insert = aux_timer.milliseconds();
    cout << "# Insertion time: " << t_insert << " millisecs" << endl;

    // save the result
    config().db()->add("insert_lookup")
                    ("type", "insert")
                    ("initial_size", (size_t) 0)
                    ("elements", N_inserts)
                    ("time", t_insert);


    aux_timer.reset(true);
    pma->build(); // in case of the Static-ABTree we build the tree only after calling #build()
    aux_timer.stop();
    size_t t_build = aux_timer.milliseconds();
    if(t_build > 0){
        cout << "# Build time: " << t_build << " millisecs" << endl;
    }
    assert(interface->size() == N_inserts);

    if(N_lookups > 0){
        size_t seed_lookups = ARGREF(uint64_t, "seed_lookups");
        cout << "Searching " << N_lookups << " elements ..." << endl;
        aux_timer.reset(true);
        do_lookups(pma, distribution.get(), seed_lookups);
        aux_timer.stop();
        uint64_t t_lookup = aux_timer.milliseconds();
        cout << "# Lookup time: " << t_lookup << " millisecs" << endl;

        config().db()->add("insert_lookup")
                        ("type", "search")
                        ("initial_size", N_inserts)
                        ("elements", N_lookups)
                        ("time", t_lookup);
    }
}

} /* namespace pma */
