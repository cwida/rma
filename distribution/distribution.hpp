/*
 * distribution.hpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
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
