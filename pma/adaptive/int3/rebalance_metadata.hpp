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

#ifndef PMA_ADAPTIVE_INT3_REBALANCE_METADATA_HPP_
#define PMA_ADAPTIVE_INT3_REBALANCE_METADATA_HPP_

#include <cinttypes>
#include "partition.hpp"

namespace pma { namespace adaptive { namespace int3 {

enum class RebalanceOperation { REBALANCE, RESIZE, RESIZE_REBALANCE };
struct RebalanceMetadata {
    RebalanceOperation m_operation; // the operation to perform
    int64_t m_window_start; // the first segment to rebalance
    int64_t m_window_length; // the number of segments to rebalance, starting from m_window_start
    int64_t m_cardinality_after; // the final cardinality
    bool m_is_insert = false;
    int64_t m_insert_key = -1;
    int64_t m_insert_value = -1;
    int64_t m_insert_segment = -1;
    VectorOfPartitions m_apma_partitions; // the cardinalities of all segments, as determined by the APMA algorithm

    RebalanceMetadata(CachedMemoryPool& pool) : m_apma_partitions(vector_of_partitions(pool)){ }

    int64_t get_cardinality_after() const {
        return m_cardinality_after;
    }

    int64_t get_cardinality_before() const {
        return is_insert() ? m_cardinality_after -1 : m_cardinality_after;
    }

    bool is_insert() const {
        return m_is_insert;
    }
};

}}} // pma::adaptive::int3

#endif /* PMA_ADAPTIVE_INT3_REBALANCE_METADATA_HPP_ */
