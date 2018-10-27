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

#ifndef GENERIC_STATIC_INDEX_HPP_
#define GENERIC_STATIC_INDEX_HPP_

#include <cinttypes>
#include <ostream>

namespace pma {

/**
 * A static index with a fixed number of indexed entries. To change the number of entries
 * the whole index needs to be rebuilt (#rebuild(N)) providing the new size of entries.
 *
 * The node size B is determined on initialisation. A node size B actually requires B -1 slots
 * in terms of space, so it is recommended to set B to a power of 2 + 1 (e.g. 65) to fully
 * exploit aligned accesses to the cache.
 */
class StaticIndex {
    const uint16_t m_node_size; // number of keys per node
    int16_t m_height; // the height of this tree
    int32_t m_capacity; // the number of segments/keys in the tree
    int64_t* m_keys; // the container of the keys
    int64_t m_key_minimum; // the minimum stored in the tree

    /**
     * Keep track of the cardinality and the height of the rightmost subtrees
     */
    struct RightmostSubtreeInfo {
        uint16_t m_root_sz; // the number of elements in the root
        uint16_t m_right_height; // the height of the rightmost subtree
    };
    constexpr static uint64_t m_rightmost_sz = 8;
    RightmostSubtreeInfo m_rightmost[m_rightmost_sz];

protected:
    // Retrieve the slot associated to the given segment
    int64_t* get_slot(uint64_t segment_id) const;

    // Dump the content of the given subtree
    void dump_subtree(std::ostream& out, int64_t* root, int height, bool rightmost, int64_t fence_min, int64_t fence_max, bool* integrity_check) const;

public:
    /**
     * Initialise the AB-Tree with the given node size and capacity
     */
    StaticIndex(uint64_t node_size, uint64_t num_segments = 1);

    /**
     * Destructor
     */
    ~StaticIndex();

    /**
     * Rebuild the tree to contain `num_segments'
     */
    void rebuild(uint64_t num_segments);

    /**
     * Set the separator key associated to the given segment
     */
    void set_separator_key(uint64_t segment_id, int64_t key);

    /**
     * Get the separator key associated to the given segment.
     * Used only for the debugging purposes.
     */
    int64_t get_separator_key(uint64_t segment_id) const;

    /**
     * Return a segment_id that contains the given key. If there are no repetitions in the indexed data structure,
     * this will be the only candidate segment for the given key.
     */
    uint64_t find(int64_t key) const noexcept;

    /**
     * Return the first segment id that may contain the given key
     */
    uint64_t find_first(int64_t key) const noexcept;

    /**
     * Return the last segment id that may contain the given key
     */
    uint64_t find_last(int64_t key) const noexcept;

    /**
     * Retrieve the minimum stored in the tree
     */
    int64_t minimum() const noexcept;

    /**
     * Retrieve the height of the current static tree
     */
    int height() const noexcept;

    /**
     * Retrieve the block size of each node in the tree
     */
    int64_t node_size() const noexcept;

    /**
     * Retrieve the memory footprint of this index, in bytes
     */
    size_t memory_footprint() const;

    /**
     * Dump the fields of the index
     */
    void dump(std::ostream& out, bool* integrity_check = nullptr) const;
    void dump() const;
};

std::ostream& operator<<(std::ostream& out, const StaticIndex& index);

} // namespace pma

#endif /* GENERIC_STATIC_INDEX_HPP_ */
