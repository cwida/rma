/*
 * experiment.cpp
 *
 *  Created on: 10 Sep 2017
 *      Author: Dean De Leo
 */


#include "experiment.hpp"

namespace pma {
Experiment::Experiment() { }
Experiment::~Experiment() { }
void Experiment::execute() {
    preprocess();
    m_timer.start();
    run();
    m_timer.stop();
    postprocess();
}
size_t Experiment::elapsed_millisecs(){
    return m_timer.milliseconds<size_t>();
}
void Experiment::preprocess() { }
void Experiment::postprocess() { }
} // namespace pma
