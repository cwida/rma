/*
 * test_jraizes.cpp
 *
 *  Created on: Jul 17, 2017
 *      Author: dleo@cwi.nl
 */

#include <iostream>
#include <memory>

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"

#include "pma/driver.hpp"
#include "pma/factory.hpp"
#include "pma/interface.hpp"

using namespace pma;
using namespace std;

TEST_CASE("sanity"){
    cout << "Driver JRaizes (it seems working)" << endl;

    pma::initialise();
    auto driver0 = factory().make_algorithm("pma_jraizes");
    REQUIRE(driver0->size() == 0);
    driver0->insert(1, 10);
    driver0->insert(3, 30);
    driver0->insert(2, 20);
    driver0->insert(4, 40);
    driver0->insert(5, 50);
    driver0->insert(6, 60);
    driver0->insert(10, 100);
    driver0->insert(11, 110);
    driver0->dump();
    REQUIRE(driver0->size() == 8);

    auto it = driver0->iterator();
    int64_t prev = -1;
    cout << "Iterator: \n";
    while(it->hasNext()){
        auto p = it->next();
        cout << "<" << p.first << ", " << p.second << ">" << endl;
        REQUIRE(prev < p.first);
        prev = p.first;
    }

    for(int64_t i = 0; i <= 12; i++){
        auto res = driver0->find(i);
        // Elements 1, 2, 3, 4, 5, 6, 10, 11 are present and should be found
        if((i >= 1 && i <= 6) || i == 10 || i == 11){
            REQUIRE(res == i * 10);
        } else {
            REQUIRE(res == -1);
        }
    }

}
