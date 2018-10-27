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

#ifndef PMA_ITERATOR_HPP_
#define PMA_ITERATOR_HPP_

#include <cinttypes>
#include <memory>
#include <utility>

namespace pma {

/**
 * [Interface]
 * Java-like iterator (`enumerator' would be more appropriate). It provides
 * an interface to iterate over each element of the range.
 */
struct Iterator {
    virtual ~Iterator();
    virtual bool hasNext() const = 0;
    virtual std::pair<int64_t, int64_t> next() = 0;
};


} // namespace pma


#endif /* PMA_ITERATOR_HPP_ */
