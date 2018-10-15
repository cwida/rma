/*
 * genzipf.c
 *
 * The source code of this file is derived from:
 * http://www.csee.usf.edu/~kchriste/tools/toolpage.html
 * https://stackoverflow.com/questions/9983239/how-to-generate-zipf-distributed-numbers-efficiently
 *
 */

//=-------------------------------------------------------------------------=
//=  Author: Kenneth J. Christensen                                         =
//=          University of South Florida                                    =
//=          WWW: http://www.csee.usf.edu/~christen                         =
//=          Email: christen@csee.usf.edu                                   =
//=-------------------------------------------------------------------------=
//= Altered by Masoud Kazemi (StackOverflow)

#include "genzipf.hpp"

#include <cassert>
#include <cmath>
#include <stdexcept>

ZipfDistribution::ZipfDistribution(double alpha, uint64_t N, uint64_t seed) : m_range(N), m_rand_gen(seed), m_sum_probs(nullptr) {
    if(alpha <= 0) throw std::invalid_argument("[ZipfDistribution] alpha <= 0");

    double c = 0; // Normalization constant    for (i=1; i<=n; i++)
    for (size_t i=1; i<=N; i++)
        c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;

    m_sum_probs = (double*) malloc((N+1)*sizeof(*m_sum_probs));
    m_sum_probs[0] = 0;
    for (size_t i=1; i<=N; i++) {
        m_sum_probs[i] = m_sum_probs[i-1] + c / pow((double) i, alpha);
    }
}

ZipfDistribution::~ZipfDistribution(){
    free(m_sum_probs); m_sum_probs = nullptr;
}

double ZipfDistribution::rand_val() {
    return std::uniform_real_distribution(0.0, 1.0)(m_rand_gen);
}

//===========================================================================
//=  Function to generate Zipf (power law) distributed random variables     =
//=    - Input: alpha and N                                                 =
//=    - Output: Returns with Zipf distributed random variable              =
//===========================================================================
uint64_t ZipfDistribution::next(){
    double z = 0;                 // Uniform random number (0 < z < 1)
    uint64_t zipf_value = 0;          // Computed exponential value to be returned
    int low, high, mid;           // Binary-search bounds

    // Pull a uniform random number (0 < z < 1)
    do { z = rand_val(); } while ((z == 0) || (z == 1));

    // Map z to the value
    low = 1, high = m_range;
    do {
        mid = floor((low+high)/2);
        if (m_sum_probs[mid] >= z && m_sum_probs[mid-1] < z) {
            zipf_value = mid;
            break;
        } else if (m_sum_probs[mid] >= z) {
            high = mid-1;
        } else {
            low = mid+1;
        }
    } while (low <= high);

    // Assert that zipf_value is between 1 and N
    assert((zipf_value >=1) && (zipf_value <= m_range));

    return zipf_value;
}



