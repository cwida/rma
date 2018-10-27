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
