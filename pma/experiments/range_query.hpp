/*
 * range_query.hpp
 *
 *  Created on: 27 Dec 2017
 *      Author: Dean De Leo
 *
 */

#ifndef PMA_RANGE_QUERY_HPP_
#define PMA_RANGE_QUERY_HPP_

#include "pma/experiment.hpp"

#include <memory>
#include <vector>

namespace pma {

class Interface;
namespace { struct ContainerKeys; } // forward decl, impl detail

/**
 * Experiment: Perform N range queries over multiple intervals
 */
struct ExperimentRangeQueryIntervals : public Experiment {
    std::shared_ptr<Interface> pma_ptr; // the actual implementation
    const size_t N_inserts; // number of elements to insert
    const size_t N_lookups; // number of scans to perform
    std::vector<double> intervals; // the intervals to try
    bool thread_pinned = false;
    std::unique_ptr<ContainerKeys> m_keys; // map the keys contained in the pma

    ExperimentRangeQueryIntervals(std::shared_ptr<Interface> pmae, size_t N_inserts, size_t N_lookups, const std::vector<double>& intervals);

    ~ExperimentRangeQueryIntervals();

    void preprocess() override;

    void run() override;
};

} // namespace pma



#endif /* PMA_RANGE_QUERY_HPP_ */
