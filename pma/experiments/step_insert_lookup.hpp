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

#ifndef PMA_STEP_INSERT_LOOKUP_HPP_
#define PMA_STEP_INSERT_LOOKUP_HPP_

#include "pma/experiment.hpp"

#include <memory>

namespace distribution { class Distribution; }

namespace pma {
class Interface;

/*****************************************************************************
 *                                                                           *
 *  Given a PMA_interface and an N_insert (e.g. 4G), the experiment          *
 *  proceeds as follows:                                                     *
 *  1. Set N = 0, step = 1024                                                *
 *  2. Insert step elements and evaluate the total insertion time            *
 *      t_insert, set N += step and store the result                         *
 *  3. Find `N_lookups' existing elements in the data structure and          *
 *      store the total search time for N                                    *
 *  4. Set step = N (to double the size of the data structure)               *
 *  5. If N + step < N_insert, go back to point 2)                           *
 *                                                                           *
 *****************************************************************************/
class ExperimentStepInsertLookup : public Experiment {
    std::shared_ptr<Interface> interface; // the data structure to evaluate
    const size_t N_inserts; // number of elements to insert
    const size_t N_lookups; // number of look up to perform
    std::unique_ptr<distribution::Distribution> distribution;
    bool thread_pinned = false; // keep track if we have pinned the thread

protected:
    void preprocess() override;

    void run() override;

public:
    ExperimentStepInsertLookup(std::shared_ptr<Interface> pma, size_t N, size_t M);

    ~ExperimentStepInsertLookup();
};

} // namespace pma
#endif /* PMA_STEP_INSERT_LOOKUP_HPP_ */
