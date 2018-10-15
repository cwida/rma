/*
 * pma.hpp
 *
 *  Extracted and adapted from
 *  https://github.com/desert0616/gpma_bfs_demo/blob/master/cpu_baseline/containers/pma_dynamic_graph.hpp
 *  Original author: Mo Sha
 *
 *  ---------------------------------------------------------------------------
 *
 *  MIT License
 *
 *  Copyright (c) 2018 Mo Sha
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in al
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE
 *
 */

#ifndef PMA_EXTERNAL_SHA_PMA_HPP_
#define PMA_EXTERNAL_SHA_PMA_HPP_

#include <cstdint>
#include <utility>
#include <vector>

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

namespace pma { namespace sha {

using ELEMENT_TYPE = std::pair<int64_t, int64_t>;

class Iterator : public ::pma::Iterator {
    int64_t real_idx_;
    const std::vector <ELEMENT_TYPE>& data_;
    const std::vector<bool>& element_exist_;
    const int64_t end;

    void move_next();

public:
    Iterator(const std::vector<ELEMENT_TYPE> &data, const std::vector<bool> &element_exist, int64_t start /* inclusive */, int64_t end /* exclusive */);

    bool hasNext() const override;
    std::pair<int64_t, int64_t> next() override;
};

class PMA : public InterfaceRQ {
    // density threshold for lower_leaf, lower_root, upper_root, upper_leaf
    // these four threshold should be monotonically increasing
    double density_lower_thres_leaf_ = 0.08;
    double density_lower_thres_root_ = 0.30;
    double density_upper_thres_root_ = 0.70;
    double density_upper_thres_leaf_ = 0.92;

    std::vector <ELEMENT_TYPE> data_;
    std::vector<bool> element_exist_;
    std::vector <ELEMENT_TYPE> buffer_;

    int64_t segment_length_;
    int64_t tree_height_;
    std::vector<int64_t> lower_element_;
    std::vector<int64_t> upper_element_;
    int64_t element_cnt_;

    void recalculate_density();
    void init_pma();
    int64_t locate_segment(int64_t key) const;
    void project_buffer(int64_t head, int64_t rear);
    void evenly_dispatch_buffer(int64_t head, int64_t rear);
    int64_t get_parent(int64_t left_location, int level) const;
    void resize(int64_t size);
    void rebalance(int64_t left_location, int level);
    void insert_pma(ELEMENT_TYPE element);

protected:

    /**
     * Find the begin & end indices for the given interval [min, max]
     */
    std::pair<int64_t, int64_t> find_interval(int64_t min, int64_t max) const;

    /**
     * Check the data is stored following the sorted order. Throw an exception in case the invariant
     * is not satisfied.
     */
    void validate() const;

public:

    /**
     * Initialise an empty PMA
     */
    PMA();

    /**
     * Insert the given <key, value> in the container
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Return the value associated to the element with the given `key', or -1 if not present.
     * In case of duplicates, it returns the value of one of the qualifying elements.
     */
    int64_t find(int64_t key) const override;

    /**
     * Return the number of elements in the container
     */
    std::size_t size() const override;

    /**
     * Find all elements in the interval [min, max]
     */
    std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

    /**
     * Scan all elements in the container
     */
    std::unique_ptr<pma::Iterator> iterator() const override;

    /**
     * Sum all elements together in the interval [min, max]
     */
    SumResult sum(int64_t min, int64_t max) const override;

    /**
     * Dump the content of the container to stdout, for debugging purposes
     */
    void dump() const override;
};




}} // namespace pma::sha

#endif /* PMA_EXTERNAL_SHA_PMA_HPP_ */
