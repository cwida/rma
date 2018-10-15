/*
 * uniform_distribution.hpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#ifndef UNIFORM_DISTRIBUTION_HPP_
#define UNIFORM_DISTRIBUTION_HPP_

#include "distribution.hpp"

namespace distribution {

/**
 * Generate a permutation of [1, N] following an uniform distribution. No elements are repeated.
 */
std::unique_ptr<Distribution> make_uniform(size_t N);


} // namespace distribution

#endif /* UNIFORM_DISTRIBUTION_HPP_ */
