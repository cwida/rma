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

#ifndef PMA_ADAPTIVE_INT2_SUM_HPP_
#define PMA_ADAPTIVE_INT2_SUM_HPP_

#include "pma/interface.hpp"
#include "storage.hpp"

namespace pma { namespace adaptive { namespace int2 {

::pma::Interface::SumResult do_sum(const Storage& storage, int64_t segment_start, int64_t segment_end, int64_t key_min, int64_t key_max);

}}} // pma::adaptive::int2


#endif /* PMA_ADAPTIVE_INT2_SUM_HPP_ */
