/*
 * bandwidth_idls.hpp
 *
 *  Created on: 24 Sep 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_EXPERIMENTS_BANDWIDTH_IDLS_HPP_
#define PMA_EXPERIMENTS_BANDWIDTH_IDLS_HPP_

#include <vector>

#include "distribution/idls_distributions.hpp"
#include "pma/experiment.hpp"

namespace pma {
class Interface;

class ExperimentBandwidthIDLS : public Experiment {
private:
    std::shared_ptr<Interface> m_pma; // actual implementation
    const size_t N_initial_inserts; // number of initial elements to insert
    const size_t N_insdel; // number of insert/delete operations, total
    const size_t N_consecutive_operations; // perform N consecutive insertions/deletions at the time
    const distribution::idls::eDistributionType m_distribution_type_insert; // the kind of distribution to employ for inserts
    const double m_distribution_param_alpha_insert; // first parameter of the distribution
    const distribution::idls::eDistributionType m_distribution_type_delete; // the kind of distribution to employ for deletes
    const double m_distribution_param_alpha_delete; // first parameter of the distribution
    const double m_distribution_param_beta; // second parameter of the distribution
    const uint64_t m_distribution_seed; // the seed to use to initialise the distribution
    std::vector<uint32_t> m_updates_per_second; // number of operations performed per second
    distribution::idls::DistributionsContainer m_keys_experiment; // the distributions to perform the experiment

    /**
     * Insert the first `N_initial_inserts' into the data structure
     */
    void run_initial_inserts();

    /**
     * Perform the bulk of the experiment, inserting and/or deleting the elements in the data structure
     */
    void run_insert_deletions();

    /**
     * Insert the given elements in the data structure
     */
    void run_updates(distribution::idls::Distribution<long>* distribution);

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
    ExperimentBandwidthIDLS(std::shared_ptr<Interface> pmae, size_t N_initial_inserts, size_t N_insdel, size_t N_consecutive_operations,
            std::string insert_distribution, double insert_alpha,
            std::string delete_distribution, double delete_alpha,
            double beta, uint64_t seed);

    /**
     * Destructor
     */
    virtual ~ExperimentBandwidthIDLS();
};

} /* namespace pma */

#endif /* PMA_EXPERIMENTS_BANDWIDTH_IDLS_HPP_ */
