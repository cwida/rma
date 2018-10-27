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
