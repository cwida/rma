

#include <iostream>
#include <memory>
#include <utility>

#define CATCH_CONFIG_MAIN
#include "third-party/catch/catch.hpp"
#include "pma/btree/stx-btree.hpp"

using namespace pma;
using namespace std;

TEST_CASE("basic"){
    auto driver0 = make_shared<STXBtree>();
    REQUIRE(driver0->size() == 0);
    driver0->insert(1, 10);
    driver0->insert(3, 30);
    driver0->insert(2, 20);
    driver0->insert(4, 40);
    driver0->insert(5, 50);
    driver0->insert(6, 60);
    driver0->insert(10, 100);
    driver0->insert(11, 110);
    driver0->insert(9, 90);
    driver0->dump();
    REQUIRE(driver0->size() == 9);

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
        // Elements 1, 2, 3, 4, 5, 6, 9, 10, 11 are present and should be found
        if((i >= 1 && i <= 6) || (i >= 9 && i <= 11)){
            REQUIRE(res == i * 10);
        } else {
            REQUIRE(res == -1);
        }
    }

}

TEST_CASE("range_query"){
    STXBtree tree;
    constexpr int64_t max = 4096;
    // insert some values in the B+ tree
    auto insert = [&tree](int64_t base){
        for(int64_t i = base; i <= max; i+=base){
            if (tree.find(i) == -1)
                tree.insert(i, i * 1000);
        }
    };

    insert(13);
    insert(7);
    insert(3);
    insert(11);
    insert(5);

    // scan all elements between 100 and 500, check they are multiples of 3, 5, 7, 11 or 13
    auto is_valid = [](int num){
        return (num % 3 == 0 || num % 5 == 0 || num % 7 == 0 || num % 11 == 0 || num % 13 == 0);
    };

    auto it = tree.find(100, 500);
    int64_t previous = -1;
    while(it->hasNext()){
        auto p = it->next();
        auto key = p.first;
        auto value = p.second;

        //        cout << "key: " << key << endl;
        REQUIRE(is_valid(key));
        REQUIRE(key * 1000 == value);

        if(previous != -1){
            // check that we didn't skip any element between the previous and the current key
            for(int64_t i = previous +1; i < key; i++){
                REQUIRE(!is_valid(i));
            }
        }

        previous = key;
    }

    REQUIRE(previous == 500); // 500 % 5 == 0
}
