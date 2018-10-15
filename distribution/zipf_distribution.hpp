/*
 * zipf_distribution.hpp
 *
 *  Created on: 4 Feb 2018
 *      Author: Dean De Leo
 */

#ifndef ZIPF_DISTRIBUTION_HPP_
#define ZIPF_DISTRIBUTION_HPP_

#include <cstddef>
#include <memory>

#include "distribution.hpp"

namespace distribution {

// Generate N numbers in the interval [1, M] << 32
std::unique_ptr<Distribution> make_zipf(double alpha, size_t N, size_t M, size_t random_seed, size_t shift = 0);


} // namespace distribution

#endif /* ZIPF_DISTRIBUTION_HPP_ */
