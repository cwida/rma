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

