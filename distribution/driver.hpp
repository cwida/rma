/*
 * driver.hpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#ifndef DISTRIBUTION_DRIVER_HPP_
#define DISTRIBUTION_DRIVER_HPP_

#include <memory>

namespace distribution {

class Distribution; // forward decl.

void initialise();

std::unique_ptr<Distribution> generate_distribution();

} // namespace distribution

#endif /* DISTRIBUTION_DRIVER_HPP_ */
