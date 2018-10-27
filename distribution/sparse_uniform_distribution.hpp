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

#ifndef SPARSE_UNIFORM_DISTRIBUTION_HPP_
#define SPARSE_UNIFORM_DISTRIBUTION_HPP_

#include <cinttypes>

#include "distribution.hpp"

namespace distribution {

/**
 * Create a sample of `num_values' values, without repetitions in the range [min, max]
 * @param min: the left bound of the interval, inclusive
 * @param max: the right bound of the interval, inclusive
 * @param num_values: the number of elements to generate
 * @param seed: random generator seed
 */
std::unique_ptr<Distribution> make_sparse_uniform(int64_t min, int64_t max, uint64_t num_values, uint64_t seed = 1);

}

#endif /* SPARSE_UNIFORM_DISTRIBUTION_HPP_ */
