/*
 * idls.hpp
 *
 *  Created on: 5 Feb 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_IDLS_HPP_
#define PMA_IDLS_HPP_

#include <vector>

#include "distribution/idls_distributions.hpp"
#include "pma/experiment.hpp"

namespace pma {
class Interface;

/**
 * Insert/Delete/Lookup/Scan experiment:
 * - 1) Insert `N_initial_inserts' into an empty PMA data structure
 * - 2) Interleave `N_insdel' inserts/deletes in groups of `N_consecutive_operations'
 * - 3) Perform `N_lookups' look ups in the final data structure
 * - 4) Perform `N_scans' range scans, with uniform distribution
 */
class ExperimentIDLS : public Experiment {
private:
    std::shared_ptr<Interface> m_pma; // actual implementation
    const size_t N_initial_inserts; // number of initial elements to insert
    const size_t N_insdel; // number of insert/delete operations, total
    const size_t N_consecutive_operations; // perform N consecutive insertions/deletions at the time
    const size_t N_lookups; // number of look ups to perform
    const size_t N_scans; // number of scans to perform
    std::vector<double> m_range_query_intervals; // size of the intervals to scan
    const distribution::idls::eDistributionType m_distribution_type_insert; // the kind of distribution to employ for inserts
    const double m_distribution_param_alpha_insert; // first parameter of the distribution
    const distribution::idls::eDistributionType m_distribution_type_delete; // the kind of distribution to employ for deletes
    const double m_distribution_param_alpha_delete; // first parameter of the distribution
    const double m_distribution_param_beta; // second parameter of the distribution
    const uint64_t m_distribution_seed; // the seed to use to initialise the distribution
    distribution::idls::DistributionsContainer m_keys_experiment; // the distributions to perform the experiment
    bool m_thread_pinned = false; // unpin the current thread at the end of the computation

    /**
     * Insert the first `N_initial_inserts' into the data structure
     */
    void run_initial_inserts();

    /**
     * Perform `count' insertions, from the given distribution
     */
    void run_inserts(distribution::idls::Distribution<int64_t>* __restrict distribution, size_t count);

    /**
     * Perform `count' deletions, from the given distribution
     */
    void run_deletions(distribution::idls::Distribution<int64_t>* __restrict distribution, size_t count);

    /**
     * Perform the lookups
     */
    void run_lookups();

    /**
     * Perform `count' scans
     */
    void run_scans(distribution::idls::Distribution<distribution::idls::ScanRange>* distribution, size_t count);

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
    ExperimentIDLS(std::shared_ptr<Interface> pmae, size_t N_initial_inserts, size_t N_insdel, size_t N_consecutive_operations,
            size_t N_lookups, size_t N_scans, const std::vector<double>& rq_intervals,
            std::string insert_distribution, double insert_alpha,
            std::string delete_distribution, double delete_alpha,
            double beta, uint64_t seed);

    /**
     * Destructor
     */
    virtual ~ExperimentIDLS();


    /**
     * Convert the distribution specified by the user to the actual IDLS distribution
     */
    static distribution::idls::eDistributionType get_distribution_type(std::string value);
};

} /* namespace pma */

#endif /* PMA_IDLS_HPP_ */
