/*
 * test_circular_array.cpp
 *
 *  Created on: 29 Aug 2018
 *      Author: Dean De Leo
 */
#include <cinttypes>

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include "circular_array.hpp"

TEST_CASE("sanity"){
    int64_t a; // temporary variable
    CircularArray<int64_t> A{/* initial capacity = */ 4};
    REQUIRE(A.empty() == true);

    A.append(1);
    A.append(2);
    A.append(3);
    A.append(4);

    REQUIRE(!A.empty());
    REQUIRE(A.size() == 4);

    a = A[0]; A.pop();
    REQUIRE(a == 1);
    a = A[0]; A.pop();
    REQUIRE(a == 2);
    REQUIRE(A.size() == 2);

    // at this point the array should be [empty, empty, 3, 4], so the next item should be placed in the first slot
    A.append(5);
    A.append(6);
    REQUIRE(A.size() == 4);

    // check the content of the array
    for(int64_t i = 3; i <= 6; i++){ // i: 3, 4, 5, 6
        REQUIRE(A[i - 3] == i);
    }

    // Resize, the current state should be: [5, 6, start: 3, 4]
    A.append(7);
    A.append(8);
    A.append(9);
    A.append(10);
    REQUIRE(A.size() == 8);

    for(int64_t i = 3; i <= 10; i++){ // i: 3, 4, 5, 6, 7, 8, 9, 10
        REQUIRE(A[i - 3] == i);
    }

    // Resize, this time with m_start < m_end
    A.append(11);
    REQUIRE(A.size() == 9);
    for(int64_t i = 3; i <= 11; i++){ // i: 3, 4, 5, 6, 7, 8, 9, 10, 11
        REQUIRE(A[i - 3] == i);
    }

    // Prepend few elements at the start
    A.prepend(2);
    A.prepend(1);
    A.prepend(0);
    REQUIRE(A.size() == 12);
    for(int64_t i = 0; i <= 11; i++){ // i: 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
        REQUIRE(A[i] == i);
    }
}
