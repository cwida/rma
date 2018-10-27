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

#ifndef BTREE_08_PACKED_MEMORY_ARRAY_HPP_
#define BTREE_08_PACKED_MEMORY_ARRAY_HPP_

#include "memory_pool.hpp"
#include "rebalance_metadata.hpp"
#include "storage.hpp"
#include "pma/density_bounds.hpp"
#include "pma/interface.hpp"
#include "pma/generic/static_index.hpp"

namespace pma { namespace v8 {

// Forward declaration
class SpreadWithRewiring;

class PackedMemoryArray8 : public Interface {
    friend class SpreadWithRewiring;
private:
    StaticIndex m_index;
    Storage m_storage;
    CachedMemoryPool m_memory_pool;
    CachedDensityBounds m_density_bounds0; // user thresholds (for num_segments<=balanced_thresholds_cutoff())
    CachedDensityBounds m_density_bounds1; // primary thresholds (for num_segmnets>balanced_thresholds_cutoff())
    bool m_segment_statistics = false; // record segment statistics at the end?

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA, assuming the given bucket if it's not full.
    void insert_common(size_t segment_id, int64_t key, int64_t value);

    // Determine the window to rebalance
    void rebalance_find_window(size_t segment_id, bool is_insert, int64_t* out_window_start, int64_t* out_window_length, int64_t* out_cardinality_after, bool* out_resize) const;

    // Determine whether to rebalance or resize the underlying storage
    RebalanceMetadata rebalance_plan(bool is_insert, int64_t window_start, int64_t window_length, int64_t cardinality_after, bool resize) const;

    // Rebalance the storage so that the density thresholds are ensured
    void rebalance(size_t segment_id, int64_t* insert_new_key, int64_t* insert_new_value);

    // Perform the rebalancing action
    void do_rebalance(const RebalanceMetadata& action);

    // Rebuild the underlying storage to hold m_elements
    void resize(const RebalanceMetadata& action);

    // Equally spread (without rewiring) the elements in the given window
    void spread_local(const RebalanceMetadata& action);

    // Helper, copy the elements from <key_from,values_from> into the arrays <keys_to, values_to> and insert the new pair <key/value> in the sequence.
    void spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value);

    // Equally spread (with rewiring) the elements in the given window
    void resize_rebalance(const RebalanceMetadata& action);

    // Retrieve the lower & higher thresholds of the calibrator tree
    std::pair<double, double> get_thresholds(int height) const;

    // Reset the thresholds for the calibrator tree
    void set_thresholds(int height_calibrator_tree);

    // Retrieve the number of segments after that the primary thresholds are used
    size_t balanced_thresholds_cutoff() const;

    // Returns an empty iterator, i.e. with an empty record set!
    std::unique_ptr<pma::Iterator> empty_iterator() const;

    // Dump the content of the storage
    void dump_storage(std::ostream& out, bool* integrity_check) const;

public:
    PackedMemoryArray8(size_t pages_per_extent);

    PackedMemoryArray8(size_t pma_segment_size, size_t pages_per_extent);

    PackedMemoryArray8(size_t index_B, size_t pma_segment_size, size_t pages_per_extent);

    virtual ~PackedMemoryArray8();

    /**
     * Insert the given key/value
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Remove the given key from the data structure. Returns its value if found, otherwise -1.
     */
    int64_t remove(int64_t key) override;

    /**
     * Find the element with the given `key'. It returns its value if found, otherwise the value -1.
     * In case of duplicates, which element is returned is unspecified.
     */
    virtual int64_t find(int64_t key) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<pma::Iterator> iterator() const override;

    // Sum all elements in the interval [min, max]
    virtual SumResult sum(int64_t min, int64_t max) const override;

    // The number of elements stored
    virtual size_t size() const override;

    // Is this container empty?
    bool empty() const noexcept;

    // Dump the content of the data structure to the given output stream (for debugging purposes)
    virtual void dump(std::ostream& out) const;

    // Dump the content of the data structure to stdout (for debugging purposes)
    virtual void dump() const override;

    // Memory footprint
    virtual size_t memory_footprint() const override;
};

// Dump
std::ostream& operator<<(std::ostream& out, const PackedMemoryArray8& pma);

} } // pma::v8

#endif /* BTREE_08_PACKED_MEMORY_ARRAY_HPP_ */
