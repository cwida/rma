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

#ifndef PMA_EXPERIMENTS_STEP_INSERT_SCAN_HPP_
#define PMA_EXPERIMENTS_STEP_INSERT_SCAN_HPP_

#include "pma/experiment.hpp"

#include <memory>

namespace distribution { class Distribution; }

namespace pma {
class Interface;

/*****************************************************************************
 *                                                                           *
 *  Insert elements in chunks of `step_size' startint from `initial_size'    *
 *  up to `final_size'. At each step, perform `num_lookups' of the data      *
 *  structure and `num_scans' of whole data structure                        *
 *                                                                           *
 *****************************************************************************/
class ExperimentStepInsertScan : public Experiment {
    std::shared_ptr<Interface> m_interface; // the data structure to evaluate
    const size_t m_initial_size; // the initial chunk of elements to insert in the data structure
    const size_t m_final_size; // the final size that the data structure should reach at the end of the experiment
    const size_t m_step_size; // the step of each iteration (e.g. 4M)
    const size_t m_num_lookups; // number of look up to perform
    const size_t m_num_scans; // number of scans to perform
//    const double m_scan_size; // the amount of the data structure to scan, in (0, 1]
    std::unique_ptr<distribution::Distribution> m_distribution; // the distribution of the keys
    bool m_thread_pinned = false; // keep track if we have pinned the thread

protected:
    void preprocess() override;

    void run() override;

public:
    ExperimentStepInsertScan(std::shared_ptr<Interface> pma, size_t initial_size, size_t final_size, size_t step_size, size_t num_lookups, size_t num_scans);

    ~ExperimentStepInsertScan();
};

} // namespace pma

#endif /* PMA_EXPERIMENTS_STEP_INSERT_SCAN_HPP_ */
