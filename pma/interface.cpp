/*
 * pma_interface.cpp
 *
 *  Created on: 17 Jul 2017
 *      Author: Dean De Leo
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


