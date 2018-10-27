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

#include "step_insert_scan.hpp"

#include <iostream>
#include <random>

#include "configuration.hpp"
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

/**
 * Insert all the elements in the permutation into the pma.
 */
static void do_inserts(Interface* pma, Distribution* elements){
    for(size_t i = 0, elements_sz = elements->size(); i < elements_sz; i++){
        auto e = elements->get(i);
        pma->insert(e.first, e.second);
    }
}

/**
 * Perform `num_lookup' searches into the given pma. Use the first N elements of the permutation for the keys to search,
 * where N = pma->size();
 */
static void do_lookups(Interface* pma, Distribution* permutation, uint64_t num_lookups, uint64_t random_seed) {
    assert(pma->size());
    mt19937_64 random_generator(random_seed);
    uniform_int_distribution<int64_t> distribution(0, pma->size() == 0 ? 0 : pma->size() -1);

    for(size_t i = 0; i < num_lookups; i++){
        pma->find(permutation->get( distribution(random_generator) ).first +1 );
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

ExperimentStepInsertScan::ExperimentStepInsertScan(shared_ptr<Interface> pma, size_t initial_size, size_t final_size, size_t step_size, size_t num_lookups, size_t num_scans) :
        m_interface(pma),
        m_initial_size(initial_size), m_final_size(final_size), m_step_size(step_size),
        m_num_lookups(num_lookups), m_num_scans(num_scans)/*, m_scan_size(scan_size)*/{
    if(pma.get() == nullptr) RAISE("The pointer data structure is NULL");
    if(initial_size > final_size) RAISE("Initial size is greater than final size: " << initial_size << " <= " << final_size);
    if(step_size == 0) RAISE("step size is 0");
//    if(scan_size > 1) RAISE("Invalid value for scan size: " << scan_size << ", expected in [0, 1]");
}

ExperimentStepInsertScan::~ExperimentStepInsertScan(){
    if(m_thread_pinned){ unpin_thread(); }
}

void ExperimentStepInsertScan::preprocess() {
    // initialize the elements that we need to add
    LOG_VERBOSE("Generating the set of elements to insert ... ");
    m_distribution = distribution::generate_distribution();

    if(m_initial_size > 0){
        auto view = m_distribution->view(0, m_initial_size);
        Timer timer;
        LOG_VERBOSE("Inserting " << m_initial_size << " to reach the initial size of the data structure...");
        timer.start();
        do_inserts(m_interface.get(), view.get());
        timer.stop();
        LOG_VERBOSE("# Insertion time (initial size): " << timer.milliseconds() << " millisecs");
    }

    pin_thread_to_cpu();
    m_thread_pinned = true;
    LOG_VERBOSE("Experiment ready to begin");
}

void ExperimentStepInsertScan::run() {
    size_t current_size = m_initial_size;
    Interface* pma = m_interface.get();
    Timer timer_insert;
    mt19937_64 random_generator(ARGREF(uint64_t, "seed_lookups"));
    uniform_int_distribution<int64_t> random_lookups(0, std::numeric_limits<int64_t>::max());
    size_t memory_footprint = pma->memory_footprint();

    while(current_size <= m_final_size){
        // Perform `m_num_lookups' lookups
        if(pma->size() > 0 && m_num_lookups > 0){
            cout << "[" << pma->size() << "] Searching " << m_num_lookups << " elements ..." << endl;
            Timer timer_lookup;
            timer_lookup.start();
            do_lookups(pma, m_distribution.get(), m_num_lookups, random_lookups(random_generator) + 13);
            timer_lookup.stop();
            uint64_t t_search = timer_lookup.milliseconds();
            cout << "[" << pma->size() << "] # Search time (total): " << t_search << " milliseconds" << endl;

            // save the result
            config().db()->add("step_insert_scan")
                            ("type", "search")
                            ("initial_size", current_size)
                            ("elements", m_num_lookups)
                            ("time", t_search)
                            ("space_usage", memory_footprint);
        }

        if(pma->size() > 0 && m_num_scans > 0){
            cout << "[" << pma->size() << "] Perfoming " << m_num_scans << " scans ..." << endl;
            Timer timer_scan;
            timer_scan.start();
            do_scans(pma, m_num_scans);
            timer_scan.stop();
            uint64_t t_scan = timer_scan.milliseconds();

            cout << "[" << pma->size() << "] # Scan time (total): " << t_scan << " milliseconds" << endl;

            // save the result
            config().db()->add("step_insert_scan")
                            ("type", "scan")
                            ("initial_size", current_size)
                            ("elements", m_num_scans)
                            ("time", t_scan)
                            ("space_usage", memory_footprint);
        }


        // Insert `step' elements for the next iteration
        size_t next_size = current_size + m_step_size;
        if(next_size <= m_final_size){
            unique_ptr<Distribution> view{ m_distribution->view(current_size, m_step_size) };
            cout << "[" << m_interface->size() << "] Inserting " << m_step_size << " elements ..." << endl;
            timer_insert.start();
            do_inserts(pma, view.get());
            timer_insert.stop();
            uint64_t t_insert = timer_insert.milliseconds();
            cout << "[" << m_interface->size() << "] # Insertion time (total): " << t_insert << " milliseconds" <<endl;

            pma->build(); // in case of the Static-ABTree we build the tree only after calling #build()
            memory_footprint = pma->memory_footprint();

            assert(pma->size() == next_size);

            // save the result
            config().db()->add("step_insert_scan")
                            ("type", "insert")
                            ("initial_size", current_size)
                            ("elements", next_size)
                            ("time", t_insert)
                            ("space_usage", memory_footprint);
        }

        current_size = next_size;
    }
}


} // namespace pma
