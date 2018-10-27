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

#include "sparse_uniform_distribution.hpp"

#include <cassert>
#include <cmath>
#include <iostream> // for debugging purposes
#include <random>
#include "cbytearray.hpp"
#include "cbyteview.hpp"
#include "errorhandling.hpp"

using namespace std;

namespace distribution {

// Vitter J. S. An efficient algorithm for sequential random sampling. Rapports de Recherche N* 624. 1987
template<typename RandomGenerator>
static int64_t random_sample(RandomGenerator& dblrand, int64_t previous_index, int64_t* m_, int64_t* N_){
    // Code extrapolated and adapted from the old randomgen utility.
    // The following is legacy code, I trust it works as it worked 4 years ago.
    int64_t N = *N_;
    int64_t m = *m_;

    int64_t qu1 = N+1-m;
    double qu1d = (double) qu1;
    double nmin1inv = 1.f / ( ((double)m) - 1.f);
    int64_t S = std::numeric_limits<int64_t>::max();

    while(true){
        // [D2] generate U, X, S
        double U, X;
        U = dblrand(); // uniform distribution in [0,1]

        do{
            X = N * (1 - pow(dblrand(), 1.f/m)); // beta distribution in [0,N] with with a=1, b=m
            S = floor(X);
        } while(S >= qu1); // N -m +1

        // [D3] Compare S with the approx of the distribution
        double y1 = exp(log( U *N / qu1d) * nmin1inv);
        double Vprime = y1 * ( - (X/N) +1) * (qu1d / (qu1d - S));
        if(Vprime <= 1) break;

        // [D4] Compare S with the actual distribution
        double y2 = 1;
        double top = N -1;
        double bottom;
        int64_t limit;
        if(m+1 > S){
            bottom = N -m;
            limit =  N -S;
        } else {
            bottom = -1.0 -S +N;
            limit = qu1;
        }
        int64_t k;
        for(k = N+1; k >= limit; k--){
            y2 = (y2 * top) / bottom;
            top--; bottom--;
        }

        if( (((double)N) / ((double) (N -X))) >= (y1 * exp(log(y2) * nmin1inv)) ){ // Accept S
            Vprime = exp( log(dblrand()) * nmin1inv);
            break; // exit from the loop
        }
    } // end loop

    // [D5] select record (S+1)
    N = N -1 -S;
    m--;

    *m_ = m;
    *N_ = N;
    return previous_index + S +1;
}


template<typename RandomGenerator>
static void generate_samples(RandomGenerator& dblrand, CByteArray& array, int64_t min, int64_t max, int64_t num_samples){
    assert(array.capacity() >= num_samples && "The capacity of the array is too small to hold `num_samples' values");

    if(num_samples == max - min +1){ // retrieve all numbers in the interval
        for(int i = 0; i < num_samples; i++){
            array[i] = min + i;
        }
    } else {
        int64_t max_index = 1+ max - min;// The maximum value that can be generated

        int64_t i = 0; // array index
        int64_t value = -1; // the first value
        int64_t N = max_index; // possible values
        while(num_samples > 0){
            value = random_sample(dblrand, value, &num_samples, &N);

            array[i] = min + value;
            i++;
        }
    }
}

// Adapted from https://en.wikipedia.org/wiki/Fisher-Yates_shuffle
template<typename RandomGenerator>
static void shuffle(RandomGenerator& dblrand, CByteArray& A){
    if(A.capacity() <= 1) return;

    for(size_t i = 0, sz = A.capacity(); i < sz; i++){
        // get a random integer in [i, n)
        int64_t j = static_cast<int64_t>(dblrand() * (sz - i)) + i; if (j == sz) j--;

//        cout << "swap(" << i << ", " << j << "): " << A[i] << " <=> " << A[j] << endl; // debug only
        swap(A[i], A[j]);
    }
}

unique_ptr<Distribution> make_sparse_uniform(int64_t min, int64_t max, uint64_t num_values, uint64_t seed){
    if(min < 0) RAISE_EXCEPTION(Exception, "Parameter min < 0");
    if(max <= min) RAISE_EXCEPTION(Exception, "Invalid arguments: max <= min");
    if(num_values < 1) RAISE_EXCEPTION(Exception, "Invalid number of entries to generate: " << num_values);
    if(num_values > max - min +1) RAISE_EXCEPTION(Exception, "Invalid range: [" << min << ", " << max << "]; not enough room to extract `" << num_values << "' samples without repetitions");

    /**
     * Wrapper to generate the random numbers
     */
    struct RandomGenerator {
        mt19937_64 m_generator;
        uniform_real_distribution<double> m_distribution;

        RandomGenerator(uint64_t seed): m_generator(seed) { }
        double operator () (){ return m_distribution(m_generator); }
    } random_generator(seed);

    unique_ptr<CByteArray> array{ new CByteArray( CByteArray::compute_bytes_per_elements(max), num_values) };

    // subtract -1 to the indices to generate, as CByteView::key automatically adds 1
    generate_samples(random_generator, *(array.get()), min -1, max -1, num_values);

    shuffle(random_generator, *(array.get()));

    return make_unique<CByteView>(move(array));
}


} // namespace distribution

