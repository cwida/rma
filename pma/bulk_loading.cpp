/*
 * bulk_loading.cpp
 *
 *  Created on: May 15, 2018
 *      Author: dleo@cwi.nl
 */

#include "bulk_loading.hpp"
#include <algorithm>

using namespace std;

namespace pma {

BulkLoading::~BulkLoading(){ };

void SortedBulkLoading::load(std::pair<int64_t, int64_t>* array, size_t array_sz){
    auto comparator = [](auto& e1, auto& e2){
        return e1.first < e2.first;
    };
    std::sort(array, array + array_sz, comparator);
    load_sorted(array, array_sz);
}

} // namespace pma


