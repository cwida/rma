/*
 * zipf_distribution.cpp
 *
 *  Created on: 4 Feb 2018
 *      Author: Dean De Leo
 */

#include "zipf_distribution.hpp"

#include <stdexcept>

#include "cbytearray.hpp"
#include "cbyteview.hpp"
#include "random_permutation.hpp"
#include "third-party/zipf/genzipf.hpp"

using namespace std;

namespace distribution {

unique_ptr<Distribution> make_zipf(double alpha, size_t N, size_t M, size_t random_seed, size_t shift){
    // validate the input parameters
    if(alpha <= 0)  throw invalid_argument("alpha <= 0");
    if(M == 0) throw invalid_argument("M == 0");

    // first, generate a random permutation of the population
    RandomPermutationParallel rp{ M, random_seed + 1003 };
    auto population = rp.get_view();

    shared_ptr<CByteArray> values{ new CByteArray(8, N) };

    ZipfDistribution zipf{alpha, M, random_seed };
    for(size_t i = 0; i < N; i++){
        uint64_t j = zipf.next() -1;
        uint64_t v = population->key(j);

        values->set_value_at(i, (v << 32) | (shift + i));
    }

    return make_unique<CByteView>(values, 0, N);
}

}
