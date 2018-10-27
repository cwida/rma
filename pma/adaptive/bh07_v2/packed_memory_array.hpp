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

#ifndef ADAPTIVE_BH07_V2_PACKED_MEMORY_ARRAY_HPP_
#define ADAPTIVE_BH07_V2_PACKED_MEMORY_ARRAY_HPP_

#include "pma/density_bounds.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "memory_pool.hpp"
#include "partition.hpp"
#include "predictor.hpp"
#if defined(PROFILING)
#include "rebalancing_profiler.hpp"
#endif
#include "static_abtree.hpp"
#include "storage.hpp"

namespace pma { namespace adaptive { namespace bh07_v2 {

class AdaptiveRebalancing; // forward decl.
class SpreadWithRewiring; // forward decl.

class APMA_BH07_v2 : public ::pma::InterfaceRQ {
    friend class AdaptiveRebalancing;
    friend class SpreadWithRewiring;
private:
    StaticABTree m_index;
    Storage m_storage;
    Predictor m_predictor;
    DensityBounds m_density_bounds;
    CachedMemoryPool m_memory_pool;
#if defined(PROFILING)
    RebalancingProfiler m_rebalancing_profiler;
#endif
    const double m_predictor_scale; // beta parameter to adjust the resizing of the predictor
    bool m_segment_statistics = false; // record segment statistics at the end?

    // Insert an element in the given segment. It assumes that there is still room available
    // It returns true if the inserted key is the minimum in the interval
    bool storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value);

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA, assuming the given bucket if it's not full.
    void insert_common(size_t segment_id, int64_t key, int64_t value);

    /**
     * Get the lower and upper threshold for the segments at the given `node_height'
     */
    std::pair<double, double> thresholds(int node_height);
    std::pair<double, double> thresholds(int node_height, int tree_height);

    /**
     * Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
     */
    bool rebalance(size_t segment_id, int64_t* insert_new_key, int64_t* insert_new_value);

    /**
     * Resize the index, double the capacity of the PMA, rebuild the index
     */
    void resize(const VectorOfPartitions& partitions);
    void resize_rewire(const VectorOfPartitions& partitions);
    void resize_general(const VectorOfPartitions& partitions);

    /**
     * Information about the new element to insert, in a spread operation
     */
    struct spread_insertion { int64_t m_key; int64_t m_value; size_t m_segment; };

    /**
     * Perform a spread operation in the traditional manner, i.e. packing all elements at the end of the window and
     * then spreading back to the final positions
     */
    void spread_two_copies(const VectorOfPartitions& partitions, size_t cardinality, size_t segment_start, size_t num_segments, spread_insertion* new_element);
    void spread_load(size_t segment_start, size_t segment_length, int64_t* __restrict keys_to, int64_t* __restrict values_to, spread_insertion* insert);
    void spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality);
    void spread_save(size_t segment_start, size_t segment_length, int64_t* keys_from, int64_t* values_from, size_t cardinality);

    /**
     * Helper, insert an element in the given segment during `spread' operation
     */
    void spread_insert_unsafe(spread_insertion* insert, int64_t* keys_to, int64_t* keys_from, int64_t* values_to, int64_t* values_from, int length);

    /**
     * Retrieve the number of elements to allocate in each partition, according to the current state of the
     * predictor and the `adaptive' strategy
     */
    VectorOfPartitions apma_partitions(int height, size_t cardinality, size_t segment_start, size_t num_segments, bool resize, bool can_fill_segments);

    /**
     * Returns an empty iterator, i.e. with an empty record set!
     */
    std::unique_ptr<::pma::Iterator> empty_iterator() const;

    /**
     * Rebuild the index from scratch, with a capacity for `num_segments'
     */
    void rebuild_index(size_t num_segments);

    /**
     * Segment statistics
     */
    decltype(auto) compute_segment_statistics() const;
    void record_segment_statistics() const;

    void dump_storage(std::ostream& out, bool* integrity_check) const;
    void dump_predictor(std::ostream& out, bool* integrity_check) const;

public:
    APMA_BH07_v2(size_t btree_block_size, size_t pma_segment_size, size_t pages_per_extent, double predictor_scale = 1.0, const DensityBounds& density_bounds = DensityBounds());

    virtual ~APMA_BH07_v2();

    /**
     * Insert the given key/value
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Find the element with the given `key'. It returns its value if found, otherwise the value -1.
     * In case of duplicates, which element is returned is unspecified.
     */
    virtual int64_t find(int64_t key) const override;

    virtual std::unique_ptr<::pma::Iterator> find(int64_t min, int64_t max) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<::pma::Iterator> iterator() const override;

    // Sum all elements in the interval [min, max]
    virtual ::pma::Interface::SumResult sum(int64_t min, int64_t max) const override;

    // The number of elements stored
    virtual size_t size() const noexcept override;

    // Is this container empty?
    bool empty() const noexcept;

    // Whether to save segment statistics, at the end, in the table `btree_leaf_statistics' ?
    void set_record_segment_statistics(bool value);

    // Retrieve the associated memory pool
    CachedMemoryPool& memory_pool();

    // Dump the content of the data structure to the given output stream (for debugging purposes)
    virtual void dump(std::ostream& out) const;

    // Dump the content of the data structure to stdout (for debugging purposes)
    virtual void dump() const override;
};


}}} // pma::adaptive::bh07_v2



#endif /* ADAPTIVE_BH07_V2_PACKED_MEMORY_ARRAY_HPP_ */
