/*
 * experiment.hpp
 *
 *  Created on: 10 Sep 2017
 *      Author: Dean De Leo
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
