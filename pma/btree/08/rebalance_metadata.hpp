/*
 * rebalance_metadata.hpp
 *
 *  Created on: Sep 6, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef BTREE_08_REBALANCE_METADATA_HPP_
#define BTREE_08_REBALANCE_METADATA_HPP_

#include <cinttypes>

namespace pma {
namespace v8 {

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

} /* namespace v8 */
} /* namespace pma */

#endif /* BTREE_08_REBALANCE_METADATA_HPP_ */
