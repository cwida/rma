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

#ifndef PMA_EXPERIMENT_HPP_
#define PMA_EXPERIMENT_HPP_

#include <string>

#include "errorhandling.hpp"
#include "timer.hpp"

namespace pma {

/**
 * Exception raised when identifying or running an experiment
 */
DEFINE_EXCEPTION(ExperimentError);

class Experiment {
protected:
    Timer m_timer; // internal timer

public:
    // Default constructor
    Experiment();

    // Virtual destructor
    virtual ~Experiment();

    // Execute the experiment
    void execute();

    // The count of elapsed millisecs to run the experiments
    virtual size_t elapsed_millisecs();

protected:
    // Invoked before executing the experiment
    virtual void preprocess();

    // Execute the actual experiment
    virtual void run() = 0;

    // Invoked after executing the experiment
    virtual void postprocess();
};

} // namespace pma

#endif /* PMA_EXPERIMENT_HPP_ */
