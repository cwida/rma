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
