/*
 * genzipf.hpp
 *
 */

#ifndef GENZIPF_HPP_
#define GENZIPF_HPP_

#include <cstddef>
#include <cstdint>
#include <random>

/**
 * Generates a number in [1, N] according to the Zipf distribution with parameter \alpha
 */
class ZipfDistribution {
    const uint64_t m_range;
    std::mt19937_64 m_rand_gen; // random generator
    double* m_sum_probs; // dynamic array

    // helper function, generates a random value in [0, 1]
    double rand_val();

public:
    ZipfDistribution(double alpha, uint64_t N, uint64_t seed = 1);

    ~ZipfDistribution();

    uint64_t next();
};



#endif /* GENZIPF_HPP_ */
