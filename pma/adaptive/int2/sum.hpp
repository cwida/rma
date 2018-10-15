/*
 * sum.hpp
 *
 *  Created on: 20 Aug 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_ADAPTIVE_INT2_SUM_HPP_
#define PMA_ADAPTIVE_INT2_SUM_HPP_

#include "pma/interface.hpp"
#include "storage.hpp"

namespace pma { namespace adaptive { namespace int2 {

::pma::Interface::SumResult do_sum(const Storage& storage, int64_t segment_start, int64_t segment_end, int64_t key_min, int64_t key_max);

}}} // pma::adaptive::int2


#endif /* PMA_ADAPTIVE_INT2_SUM_HPP_ */
