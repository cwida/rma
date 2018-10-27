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

#ifndef PMA_INSERT_LOOKUP_HPP_
#define PMA_INSERT_LOOKUP_HPP_

#include "pma/experiment.hpp"

#include <memory>

namespace distribution { class Distribution; }

namespace pma {
class Interface;

/**
 * Insert `N' elements in the data structure, then perform `M' lookups following
 * an uniform distribution of the contained elements
 */
class ExperimentInsertLookup : public Experiment {
private:
    std::shared_ptr<Interface> interface; // the data structure to evaluate
    const size_t N_inserts; // number of elements to insert
    const size_t N_lookups; // number of look up to perform
    std::unique_ptr<distribution::Distribution> distribution;
    bool thread_pinned = false; // keep track if we have pinned the thread


    void do_inserts(Interface* pma, distribution::Distribution* distribution);

    void do_lookups(Interface* pma, distribution::Distribution* distribution, size_t seed_lookups);

protected:
    /**
     * Initialise the distribution
     */
    void preprocess() override;

    /**
     * Execute the experiment
     */
    void run() override;

public:
    /**
     * Initialise the experiment
     * @param pma the data structure to evaluate
     * @param N the number of inserts to perform
     * @param M the number of lookups to perform
     */
    ExperimentInsertLookup(std::shared_ptr<Interface> pma, size_t N, size_t M);

    /**
     * Destructor
     */
    virtual ~ExperimentInsertLookup();
};

} /* namespace pma */

#endif /* PMA_INSERT_LOOKUP_HPP_ */
