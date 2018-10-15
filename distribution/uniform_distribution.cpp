/*
 * uniform_distribution.cpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#include "uniform_distribution.hpp"

#include "random_permutation.hpp"

namespace distribution {

/**
 * Generate a permutation of [1, N] following an uniform distribution. No elements are repeated.
 */
std::unique_ptr<Distribution> make_uniform(size_t N){
    RandomPermutationParallel rp;
    rp.compute(N);
    auto view = rp.get_view();
    view->set_dense(true);
    return view;
}


} // namespace distribution


