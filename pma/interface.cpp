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

#include "interface.hpp"

#include <limits>
#include "errorhandling.hpp"
#include "iterator.hpp"

using namespace std;

namespace pma {

Interface::~Interface() { }

void Interface::build(){ };


int64_t Interface::remove(int64_t key){
    RAISE_EXCEPTION(Exception, "Method ::remove(int64_t key) not supported!");
}

size_t Interface::memory_footprint() const{
    return 0;
}

InterfaceRQ::~InterfaceRQ(){ }

std::unique_ptr<Iterator> InterfaceRQ::iterator() const {
    return find(numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());
}

Iterator::~Iterator(){ }

std::ostream& operator<<(std::ostream& out, const Interface::SumResult& sum){
    out << "{SUM, first_key: " << sum.m_first_key << ", last_key: " << sum.m_last_key << ", "
            "num_elements: " << sum.m_num_elements << ", sum_keys: " << sum.m_sum_keys << ", "
            "sum_values: " << sum.m_sum_values << "}";
    return out;
}

} // namespace pma


