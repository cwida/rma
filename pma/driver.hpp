/*
 * driver.hpp
 *
 *  Created on: 26 Dec 2017
 *      Author: Dean De Leo
 */

#ifndef PMA_DRIVER_HPP_
#define PMA_DRIVER_HPP_

#include <memory>
namespace distribution { class Distribution; }

namespace pma {

void initialise();

void execute();

void prepare_parameters();

} // namespace pma
#endif /* PMA_DRIVER_HPP_ */
