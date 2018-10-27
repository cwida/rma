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

#ifndef ADAPTIVE_INT1_STATIC_ABTREE_HPP_
#define ADAPTIVE_INT1_STATIC_ABTREE_HPP_

#include <cinttypes>
#include <cstddef>

namespace pma { namespace adaptive { namespace int1 {

struct StaticABTree {
    const size_t B; // segment size
    int16_t m_cardinality_root; // number of elements in the root
    int16_t m_height; // the height of this tree
    int32_t m_capacity; // the number of nodes in the tree
    int32_t m_offset_leaves; // the position in m_keys where the leaves start
    int64_t* m_keys; // the container of the keys
    int64_t m_key_minimum; // record the minimum in the tree.

    // Initialise the AB-Tree with the given node size
    StaticABTree(size_t segment_sz);

    // Allocate the array for the keys
    void realloc_keys(); // use m_capacity to determine the number of nodes
    void realloc_keys(size_t num_nodes);
};

}}} // pma::adaptive::int1

#endif /* ADAPTIVE_INT1_STATIC_ABTREE_HPP_ */
