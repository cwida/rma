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

#ifndef DISTRIBUTION_HPP_
#define DISTRIBUTION_HPP_

#include <cinttypes>
#include <cstddef>
#include <memory>
#include <ostream>
#include <utility>

namespace distribution {

using KeyValue = std::pair<int64_t, int64_t>;

class Distribution {
public:
    virtual ~Distribution();

    virtual size_t size() const = 0;
    virtual KeyValue get(size_t index) const;
    virtual int64_t key(size_t index) const = 0;

//    virtual std::unique_ptr<Distribution> sort() = 0;

    virtual std::unique_ptr<Distribution> view(size_t shift);

    virtual std::unique_ptr<Distribution> view(size_t start, size_t length) = 0;

    /**
     * Whether the set of generated numbers is a sequence with no gaps and no duplicates, containing
     * all numbers of the interval requested.
     * e.g. [6, 8, 7, 4, 5] is a dense sequence for the interval [5,8].
     */
    virtual bool is_dense() const;
};


std::ostream& operator << (std::ostream& out, const KeyValue& k);

} // namespace distribution

#endif /* DISTRIBUTION_HPP_ */
