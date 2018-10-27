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


