/*
 * distribution.cpp
 *
 *  Created on: 17 Jan 2018
 *      Author: Dean De Leo
 */

#include "distribution.hpp"

/*****************************************************************************
 *                                                                           *
 *   Constructors                                                            *
 *                                                                           *
 *****************************************************************************/
namespace distribution {

Distribution::~Distribution(){ }

KeyValue Distribution::get(size_t index) const {
    int64_t k = key(index);
    return KeyValue{k, k * 10};
}

std::unique_ptr<Distribution> Distribution::view(size_t shift){ return view(shift, size() - shift); }

// default, conservative value
bool Distribution::is_dense() const { return false; }



std::ostream& operator << (std::ostream& out, const KeyValue& k){
    out << "<key: " << k.first << ", value: " << k.second << ">";
    return out;
}

} // namespace distribution

