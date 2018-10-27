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

#ifndef PMA_EXPERIMENTS_STEP_IDLS_HPP_
#define PMA_EXPERIMENTS_STEP_IDLS_HPP_

#include <limits>
#include <vector>

#include "distribution/idls_distributions.hpp"
#include "pma/experiment.hpp"

namespace pma {
class Interface;

class ExperimentStepIDLS : public Experiment {
private:
    std::shared_ptr<Interface> m_interface; // the data structure to evaluate
    const size_t m_initial_size; // the initial chunk of elements to insert in the data structure
    const size_t m_final_size; // the final size that the data structure should reach at the end of the experiment
    const size_t m_step_size; // the step of each iteration (e.g. 4M)
    const size_t m_num_scans; // number of scans to perform

    const distribution::idls::eDistributionType m_distribution_type_insert; // the kind of distribution to employ for inserts
    const double m_distribution_param_alpha_insert; // first parameter of the distribution
    const distribution::idls::eDistributionType m_distribution_type_delete; // the kind of distribution to employ for deletes
    const double m_distribution_param_alpha_delete; // first parameter of the distribution
    const double m_distribution_param_beta; // second parameter of the distribution
    const uint64_t m_distribution_seed; // the seed to use to initialise the distribution
    std::vector<uint32_t> m_updates_per_second; // number of operations performed per second
    distribution::idls::DistributionsContainer m_keys_experiment; // the distributions to perform the experiment

    bool m_thread_pinned = false; // keep track if we have pinned the thread

    /**
     * Insert the first `N_initial_inserts' into the data structure
     */
    void run_initial_inserts();

    /**
     * Perform the bulk of the experiment, inserting and/or deleting the elements in the data structure
     */
    void run_insert_deletions();

    /**
     * Insert/delete the given elements in the data structure
     */
    template<bool do_inserts>
    void run_updates(distribution::idls::Distribution<long>* distribution, int64_t max_step = std::numeric_limits<int64_t>::max());

protected:
    /**
     * Initialise the experiment. Compute the keys required for the insert/delete/lookup/range queries operations.
     */
    void preprocess() override;

    /**
     * Execute the experiment
     */
    void run() override;

public:
    ExperimentStepIDLS(std::shared_ptr<Interface> pma, size_t initial_size, size_t final_size, size_t step_size, size_t num_scans,
            std::string insert_distribution, double insert_alpha,
            std::string delete_distribution, double delete_alpha,
            double beta, uint64_t seed);

    /**
     * Destructor
     */
    virtual ~ExperimentStepIDLS();
};

} /* namespace pma */



#endif /* PMA_EXPERIMENTS_STEP_IDLS_HPP_ */
