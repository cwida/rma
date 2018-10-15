/*
 * test_random_permutation.cpp
 *
 *  Created on: Oct 30, 2017
 *      Author: dleo@cwi.nl
 */

#include <cstring> // memset
#include <iostream>

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include "distribution/cbytearray.hpp"
#include "distribution/random_permutation.hpp"

using namespace distribution;
using namespace std;

TEST_CASE("CByteArray"){
    int64_t values[] = {10, 16777215, 16777214, 16777213, 0, 1005, 2, 7192023};
    const size_t values_sz = sizeof(values) / sizeof(values[0]);
    REQUIRE(values_sz == 8);

    SECTION("explicit operator"){
        CByteArray c(3, values_sz);
        REQUIRE(c.capacity() == values_sz);

        // Assign the values
        for(int i = 0; i < values_sz; i++){
            c.set_value_at(i, values[i]);
        }

        // Read the values back
        for(int i = 0; i < values_sz; i++){
            REQUIRE(c.get_value_at(i) == values[i]);
        }
    }

    SECTION("brackets operator"){
        CByteArray d(3, values_sz);
        REQUIRE(d.capacity() == values_sz);

        // Assign the values
        for(int i = 0; i < values_sz; i++){
            d[i] = values[i];
        }

        // Read the values back
        for(int i = 0; i < values_sz; i++){
            REQUIRE(d[i] == values[i]);
        }
    }
}



TEST_CASE("RandomPermutationCompressed"){
    // these values depend on the seed passed to the ctor of RandomPermutationCompressed
//    int64_t values[] = {7, 8, 4, 5, 1, 3, 2, 6};
//    const size_t values_sz = sizeof(values) / sizeof(values[0]);
//    REQUIRE(values_sz == 8);
    constexpr size_t values_sz = 8;
    int outcomes[values_sz];
    memset(outcomes, 0, sizeof(int) * values_sz);

    RandomPermutationCompressed* rnd = new RandomPermutationCompressed(values_sz, /* seed = */ 2);
    REQUIRE(rnd->size() == values_sz);
    for(int i = 0; i < rnd->size(); i++){
        auto p = rnd->get(i);

        // So apparently we get different results whether we are using glibc or LLVM libc++
//        REQUIRE(p.first == values[i]);
//        REQUIRE(p.second == values[i] * 10);
        REQUIRE(p.first > 0);
        REQUIRE(p.first <= values_sz);
        REQUIRE(p.second == p.first * 10);
        outcomes[p.first -1]++;
    }

    delete rnd; rnd = nullptr;

    // check that each value has been seen exactly once
    for(size_t i = 0; i < values_sz; i++){
        REQUIRE(outcomes[i] == 1);
    }
}

TEST_CASE("RandomPermutationParallel"){
    constexpr size_t values_sz = 1048576; /* 1 M */

    RandomPermutationParallel* rnd = new RandomPermutationParallel();
    rnd->compute(values_sz, 2);


    int outcomes[values_sz];
    memset(outcomes, 0, sizeof(int) * values_sz);

    REQUIRE(rnd->size() == values_sz);
    for(size_t i = 0, sz = rnd->size(); i < sz; i ++){
        auto p = rnd->get(i);
        REQUIRE(p.first > 0);
        REQUIRE(p.first <= values_sz);
        REQUIRE(p.second == p.first * 10);
        outcomes[p.first -1]++;
    }

    // check that each value has been seen exactly once
    for(size_t i = 0; i < values_sz; i++){
        REQUIRE(outcomes[i] == 1);
    }
}

