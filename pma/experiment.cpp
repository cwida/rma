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
