/*
 * bulk_loading.hpp
 *
 *  Created on: 18 May 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_EXPERIMENTS_BULK_LOADING_HPP_
#define PMA_EXPERIMENTS_BULK_LOADING_HPP_

#include <memory>

#include "pma/experiment.hpp"

namespace distribution{ class Distribution; } // forward declaration

namespace pma {

class BulkLoading; // Forward declaration
class Interface; // Forward declaration

/**
 * Experiment: load the data structure in batches
 */
class ExperimentBulkLoading : public Experiment {
    std::shared_ptr<Interface> m_interface; // the actual implementation
    const size_t m_initial_size; // the initial size of the interface before the batches are loaded. During the preprocess step, the experiment will insert `m_initial_size' elements
                                 // according to a uniform distribution
    const size_t m_batch_size; // the size of each batch to load
    const size_t m_num_batches; // the number of batches to load
    std::unique_ptr<distribution::Distribution> m_distribution; // the distribution for the batches to load
    bool m_initial_size_uniform; // whether to load the first `m_initial_elements' following an uniform distribution
    bool m_thread_pinned = false; // record whether the thread has been pinned

private:
    /**
     * Cast from the PMA interface to the BulkLoading interface. It raises an exception if the cast is not allowed.
     */
    static std::shared_ptr<BulkLoading> get_bulk_loading_interface(std::shared_ptr<Interface> interface);

    /**
     * Initialise the random generator seed from the user parameters
     */
    static uint64_t random_generator_seed();

    /**
     * Get the maximum value for the uniform distribution
     */
    static uint32_t random_generator_max();


    /**
     * Preload `m_initial_size' elements into the data structure
     */
    void preload(distribution::Distribution* distribution);
    void preload(); // assume a uniform distribution

    /**
     * Load the elements in `m_num_batches' batches of size `m_batch_size'
     */
    void run_load();

    /**
     * Insert `m_num_batches' the elements one by one, using the traditional interface. It implies that `m_batch_size' == 1
     */
    void run_insert();

protected:
    /**
     * Pre-load the data structure with the initial keys. Implicitly invoked by Experiment::execute()
     */
    void preprocess() override;

    /**
     * Execute the experiment. Implicitly invoked by Experiment::execute()
     */
    void run() override;


public:
    /**
     * Instantiate the experiment
     * @param interface the data structure to evaluate. It must inherit the interface pma::BulkLoading
     * @param initial_size the number of elements to preload in the data structure before executing the experiment
     * @param batch_size the size of each batch, in terms of number of elements
     * @param num_batches the number of batches to load
     * @param initial_size_uniform whether to load the first `initial_size' elements following an uniform distribution
     */
    ExperimentBulkLoading(std::shared_ptr<Interface> interface, size_t initial_size, size_t batch_size, size_t num_batches, bool initial_size_uniform);

    /**
     * Destructor
     */
    ~ExperimentBulkLoading();
};

} // namespace pma


#endif /* PMA_EXPERIMENTS_BULK_LOADING_HPP_ */
