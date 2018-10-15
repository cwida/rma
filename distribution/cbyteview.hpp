/*
 * cbyteview.hpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#ifndef CBYTEVIEW_HPP_
#define CBYTEVIEW_HPP_

#include "cbytearray.hpp"
#include "distribution.hpp"

#include <memory>

namespace distribution {

class CByteView : public Distribution {
protected:
    std::shared_ptr<CByteArray> container_ptr; // reference counting
    CByteArray* container; // avoid the overhead of reference counting
    size_t begin; // inclusive
    size_t end; // exclusive
    bool dense; // is the sequence dense?

    CByteView();

public:
    /**
     * Create a view for the whole container
     */
    CByteView(std::shared_ptr<CByteArray> container);

    /**
     * Create a view for the container in [begin, end)
     */
    CByteView(std::shared_ptr<CByteArray> container, size_t begin, size_t end);

    size_t size() const override;

    KeyValue get(size_t index) const override;
    int64_t key(size_t index) const override;

    void sort();

    std::unique_ptr<Distribution> view(size_t start, size_t length) override;

    void set_dense(bool value);

    bool is_dense() const override;
};

} // namespace distribution

#endif /* CBYTEVIEW_HPP_ */
