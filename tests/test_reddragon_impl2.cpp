/*
 * test_reddragon_impl2.cpp
 *
 *  Created on: Jul 17, 2017
 *      Author: dleo@cwi.nl
 */

#include <iostream>
#include <memory>

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include "pma/external.hpp"

using namespace pma;
using namespace std;

TEST_CASE(" RedDragon (impl2)"){
    cout << "Driver RedDragon2 (it seems working)" << endl;

    auto driver2 = make_shared<PMA_Menghani_2>(8);
    REQUIRE(driver2->size() == 0);
    driver2->insert(1, 10);
    driver2->insert(3, 30);
    driver2->insert(2, 20);
    driver2->insert(4, 40);
    driver2->insert(5, 50);
    driver2->insert(6, 60);
    driver2->insert(10, 100);
    driver2->insert(11, 110);
    driver2->dump();
    REQUIRE(driver2->size() == 8);

    auto it = driver2->iterator();
    int64_t prev = -1;
    cout << "Iterator: \n";
    while(it->hasNext()){
        auto p = it->next();
        cout << "<" << p.first << ", " << p.second << ">" << endl;
        REQUIRE(prev < p.first);
        prev = p.first;
    }

    for(int64_t i = 0; i <= 12; i++){
        auto res = driver2->find(i);
        // Elements 1, 2, 3, 4, 5, 6, 10, 11 are present and should be found
        if((i >= 1 && i <= 6) || i == 10 || i == 11){
            REQUIRE(res == i * 10);
        } else {
            REQUIRE(res == -1);
        }
    }

}
