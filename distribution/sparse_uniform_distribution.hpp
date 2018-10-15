/*
 * sparse_uniform_distribution.hpp
 *
 *  Created on: 8 Apr 2018
 *      Author: Dean De Leo
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
