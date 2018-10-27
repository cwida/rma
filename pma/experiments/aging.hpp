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

#ifndef PMA_EXPERIMENTS_AGING_HPP_
#define PMA_EXPERIMENTS_AGING_HPP_

#include <fstream>
#include <string>

#include "pma/experiment.hpp"
#include "abtree/abtree.hpp"

namespace pma {

/**
 * Similar to IDLS, perform batches of insertions/deletions followed by full scans. Aimed at showing the `aging' effect
 * in AB-Trees.
 * 1- First load `initial_size' elements
 * 2- Perform `total_operations' in the form:
 *      2a- Set c = 0;
 *      2b- While count < total_operations
 *      2c- Perform `batch_size' insertions, set count += `batch_size'.
 *      2d- Perform `batch_size' deletions, set count += `batch_size'.
 *      2e- Perform `scan_num_trials' scans
 *      2d- Record the total time for insertions, deletions and scans, in the table aging
 *      2e- End while, goto 2b.
 */
class ExperimentAging : public Experiment {
    std::shared_ptr<abtree::ABTree> m_instance; // the data structure we are testing
    const size_t m_initial_size; // the initial number of elements to load
    const size_t m_total_operations; // total number of insert/delete operations
    const size_t m_batch_size; // interleave `m_batch_size' insert/delete operation at time, then perform the scans
    const size_t m_scan_warmup; // before starting the experiment, perform `m_scan_warmup' iterations to warm up the processor
    const size_t m_scan_trials; // the number of scans to perform each time
    bool m_thread_pinned = false; // record whether the thread has been pinned

    // distributions
    const std::string m_temporary_folder; // the folder where to save the generated keys
    std::string m_path_initial_load; // absolute path to the file containing the initial keys to load
    std::string m_path_insert_keys; // absolute path to the file containing the keys to insert
    std::string m_path_delete_keys; // absolute path to the file containing the keys to delete
    const uint64_t m_distribution_seed; // the seed to use to initialise the distribution

private:

    /**
     * Resolve the path to the `generator' utility. The keys are generated in a different process, to avoid polluting & segmenting the memory
     * space of the current process.
     */
    std::string get_path_generator();

    /**
     * Generate the keys for the experiment. As side effect, it initialises the variables m_path_initial_load, m_path_insert_keys, m_path_delete_keys;
     */
    void generate_keys();

    /**
     * Load the initial elements in the data structure
     * @return the time required to load the initial elements, in microseconds
     */
    size_t run_load();

    /**
     * Read `output_sz' keys from the input file, and store in the array `output'
     */
    void fill(std::fstream& input, int64_t* output, size_t output_sz);

    /**
     * Perform `count' insertions, from the given distribution
     */
    void run_inserts(int64_t* __restrict input_keys, size_t count);

    /**
     * Perform `count' deletions, from the given distribution
     */
    void run_deletions(int64_t* __restrict input_keys, size_t count);

    /**
     * Perform `count' scans
     */
    void run_scans(size_t count);

    /**
     * Perform `m_scan_warmup' iterations to warm up the processor
     */
    void run_warmup();

protected:
    /**
     * Initialise the experiment. Compute the keys required for the insert/delete/lookup/range queries operations.
     */
    void preprocess() override;

    /**
     * Execute the experiment
     */
    void run() override;

    /*
     * Release the pinning for the current thread
     */
    void postprocess() override;

public:
    ExperimentAging(std::shared_ptr<Interface> abtree_instance, size_t initial_size, size_t total_operations, size_t batch_size,
            size_t scan_warmup, size_t scan_num_trials,
            const std::string& temporary_folder,
            uint64_t seed);

    ~ExperimentAging();
};

}



#endif /* PMA_EXPERIMENTS_AGING_HPP_ */
