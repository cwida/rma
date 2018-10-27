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

#include "static_abtree.hpp"

#include <cstdlib> // posix_memalign
#include <limits>
#include "errorhandling.hpp"
#include "miscellaneous.hpp" // hyperceil

using namespace std;

namespace pma { namespace adaptive { namespace int1 {

StaticABTree::StaticABTree(size_t btree_block_size) : B(hyperceil(btree_block_size)), m_keys(nullptr){
    m_cardinality_root = 0;
    m_height = 1;
    m_capacity = 1;
    /*m_keys = */ realloc_keys();
    m_offset_leaves = 0;
    m_key_minimum = numeric_limits<decltype(m_key_minimum)>::min();
}

void StaticABTree::realloc_keys() {
    realloc_keys(m_capacity);
}

void StaticABTree::realloc_keys(size_t num_nodes) {
    free(m_keys); m_keys = nullptr;

    int64_t* ptr (nullptr);
    size_t request_sz = B * sizeof(int64_t) * num_nodes;
    int rc = posix_memalign((void**) &ptr, /* alignment */ 64,  /* size */ request_sz);
    if(rc != 0) {
        RAISE_EXCEPTION(Exception, "[StaticABTree::realloc_keys] It cannot obtain a chunk of aligned memory. " <<
                "Requested size: " << request_sz << " for " << num_nodes << " ABTree nodes");
    }

    m_keys = ptr;
}

}}} // pma::adaptive::int1
