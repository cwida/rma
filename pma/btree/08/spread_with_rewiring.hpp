/*
 * spread_with_rewiring.hpp
 *
 *  Created on: Sep 6, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef BTREE_08_SPREAD_WITH_REWIRING_HPP_
#define BTREE_08_SPREAD_WITH_REWIRING_HPP_

#include <cinttypes>
#include <cstddef>
#include <deque>

namespace pma {
namespace v8 {

// Forward declarations
class PackedMemoryArray8;

class SpreadWithRewiring {
    // user parameters:
    PackedMemoryArray8& m_instance; // underlying instance
    const size_t m_window_start; // the first segment
    const size_t m_window_length; // the number of consecutive segments in the window being spread
    const size_t m_cardinality; // the total number of elements in the window being rebalanced
    const size_t m_segments_per_extent; // total number of segments per extent

    int64_t m_insert = false;
    int64_t m_insert_key = -1;
    int64_t m_insert_value = -1;

    // internal state
    int64_t m_position = -1; // current position in the source segment
    struct Extent2Rewire{ int64_t m_extent_id; int64_t* m_buffer_keys; int64_t* m_buffer_values; };
    std::deque<Extent2Rewire> m_extents_to_rewire; // a list of extents to be rewired

    size_t get_segment_capacity() const; // the capacity of a single segment, in terms of number of elements
    int64_t position2segment(int64_t position) const;
    int64_t position2extent(int64_t position) const;
    int64_t extent2segment(int64_t extent) const;
    int64_t get_current_extent() const;
    size_t get_offset(int64_t relative_extent_id) const;
    int64_t* get_start_address(int64_t* array, int64_t relative_extent_id) const;
    void acquire_free_space(int64_t** space_keys, int64_t** space_values);
    void rewire_keys(int64_t* addr1, int64_t* addr2);
    void rewire_values(int64_t* addr1, int64_t* addr2);
    void reclaim_past_extents();
    void spread_elements(int64_t* __restrict destination_keys, int64_t* __restrict destination_values, size_t extent_id, size_t num_elements);
    void spread_extent(int64_t extent_id, size_t num_elements);
    void spread_window();
    void update_segment_sizes();
    void insert(int64_t segment_id);
    void update_index();
public:
    /**
     * Init a new rebalancing operation
     * @param instance the data structure to rebalance
     * @param window_start the first segment for the window to rebalance
     * @param window_length the number of segments in the window
     * @param cardinality the number of elements in the data structure, excluding the new element to be inserted
     */
    SpreadWithRewiring(PackedMemoryArray8* instance, size_t window_start, size_t window_length, size_t cardinality);

    /**
     * Insert a new element while rebalancing
     */
    void set_element_to_insert(int64_t key, int64_t value);

    /**
     * Set the start position for the input
     */
    void set_start_position(size_t position);

    /**
     * Perform the rebalancing operation
     */
    void execute();
};

} /* namespace v8 */
} /* namespace pma */

#endif /* BTREE_08_SPREAD_WITH_REWIRING_HPP_ */
