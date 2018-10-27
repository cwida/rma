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
#ifndef ADAPTIVE_INT1_PACKED_MEMORY_ARRAY_HPP_
#define ADAPTIVE_INT1_PACKED_MEMORY_ARRAY_HPP_

// gather rebalancing statistics
//#define PROFILING

#include <memory>
#include <random>

#include "pma/density_bounds.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "detector.hpp"
#include "knobs.hpp"
#include "memory_pool.hpp"
#include "partition.hpp"
#if defined(PROFILING)
#include "rebalancing_profiler.hpp"
#endif
#include "static_abtree.hpp"
#include "storage.hpp"

namespace pma { namespace adaptive { namespace int1 {

class Weights;

class PackedMemoryArray : public pma::InterfaceRQ {
    friend class Weights;
private:
    StaticABTree m_index;
    Storage m_storage;
    Knobs m_knobs; // APMA settings
    Detector m_detector;
    CachedDensityBounds m_density_bounds;
    CachedMemoryPool m_memory_pool;
#if defined(PROFILING)
    RebalancingProfiler m_rebalancing_profiler;
#endif
    std::default_random_engine m_random_sampler; // to decide whether to forward an update to the predictor
    static std::uniform_int_distribution<int> m_sampling_distribution;
    bool m_segment_statistics = false; // record segment statistics at the end?

    // Find the bucket in the PMA that contains the given key
    size_t find_bucket_any(int64_t key) const noexcept;
    size_t find_bucket_min(int64_t key) const noexcept;
    size_t find_bucket_max(int64_t key) const noexcept;

    // Find the interval in the node for the given given key
    size_t scan_node_any(int64_t* __restrict node, size_t node_sz, int64_t key) const noexcept;
    size_t scan_node_min(int64_t* __restrict node, size_t node_sz, int64_t key) const noexcept;
    size_t scan_node_max(int64_t* __restrict node, size_t node_sz, int64_t key) const noexcept;

    // Insert an element in the given segment. It assumes that there is still room available
    // It returns true if the inserted key is the minimum in the interval
    bool storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value);
    template<bool record_update>
    bool storage_insert_unsafe0(size_t segment_id, int64_t key, int64_t value);

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA, assuming the given bucket if it's not full.
    void insert_common(size_t segment_id, int64_t key, int64_t value);

    // Set the pivot for the given segment ID in the B+ tree index
    void set_pivot(size_t segment_id, int64_t key);

    // [Debug only] Given a segment ID, retrieve its pivot. The minimum of the segment and the related
    // entry in the index, its pivot, should be equal.
    int64_t get_pivot(size_t segment_id) const;

    /**
     * Get the lower and upper threshold for the segments at the given `node_height'
     */
    std::pair<double, double> thresholds(int node_height);
    std::pair<double, double> thresholds(int node_height, int tree_height);

    /**
     * Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
     * @return true if the operation is a ::spread and the new key has been inserted, false otherwise
     */
    bool rebalance(size_t segment_id, int64_t* insert_new_key, int64_t* insert_new_value);

    /**
     * Resize the index, double the capacity of the PMA, rebuild the index
     */
    void resize(const VectorOfPartitions& partitions, bool is_insert);

    /**
     * Spread the elements in the segments [segment_start, segment_start + num_segments)
     */
    struct spread_insertion { int64_t m_key; int64_t m_value; size_t m_segment; };
    struct spread_detector_record{ int64_t m_position; int64_t m_predecessor; int64_t m_successor; };
    void spread_evenly(size_t segment_start, size_t num_segments, size_t cardinality, spread_insertion* insert);
    void spread_unevenly(const VectorOfPartitions& partitions, size_t segment_start, size_t num_segments, size_t cardinality, spread_insertion* insert);
    void spread_load(size_t segment_start, size_t segment_length, int64_t* __restrict keys_to, int64_t* __restrict values_to, spread_insertion* insert);
    void spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record);
    void spread_save(size_t segment_start, size_t segment_length, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record);

    /**
     * Helper, create the data structure detector record, based on the position the new key has been inserted
     */
    spread_detector_record spread_create_detector_record(int64_t* keys, int64_t size, int64_t position);

    /**
     * Helper, insert an element in the given segment during `spread' operation
     * @return the position where the element has been inserted
     */
    size_t spread_insert_unsafe(spread_insertion* insert, int64_t* keys_to, int64_t* keys_from, int64_t* values_to, int64_t* values_from, int length);

    /**
     * Returns an empty iterator, i.e. with an empty record set!
     */
    std::unique_ptr<::pma::Iterator> empty_iterator() const;

    /**
     * Check whether to record the update in the detector
     */
    bool record_sample_update() noexcept;

    /**
     * Dump helpers
     */
    void dump_node(std::ostream& out, size_t base, size_t offset, size_t level_capacity, int depth, bool* integrity_check) const;
    void dump_storage(std::ostream& out, bool* integrity_check) const;
    void dump_predictor(std::ostream& out) const;

    /**
     * Segment statistics
     */
    decltype(auto) compute_segment_statistics() const;
    void record_segment_statistics() const;

protected:
    // Helper for the class Weights
    // Find the position of the key in the given segment, or return -1 if not found.
    int find_key(size_t segment_id, int64_t key) const noexcept;

public:
    PackedMemoryArray();

    PackedMemoryArray(size_t pma_segment_size);

    PackedMemoryArray(size_t index_B, size_t pma_segment_size);

    virtual ~PackedMemoryArray();

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

    virtual std::unique_ptr<::pma::Iterator> find(int64_t min, int64_t max) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<::pma::Iterator> iterator() const override;

    // Sum all elements in the interval [min, max]
    virtual ::pma::Interface::SumResult sum(int64_t min, int64_t max) const override;

    // The number of elements stored
    virtual size_t size() const noexcept override;

    // Is this container empty?
    bool empty() const noexcept;

    // Dump the content of the data structure to the given output stream (for debugging purposes)
    virtual void dump(std::ostream& out) const;

    // Dump the content of the data structure to stdout (for debugging purposes)
    virtual void dump() const override;

    // Whether to save segment statistics, at the end, in the table `btree_leaf_statistics' ?
    void set_record_segment_statistics(bool value);

    // Accessor to the underlying memory pool
    CachedMemoryPool& memory_pool();

    // Accessor to the underlying predictor/detector
    Detector& detector();

    // Accessor to the density object
    CachedDensityBounds& densities();

    // Accessor to the APMA settings
    Knobs& knobs();

    // Accessor, the max capacity of each segment
    size_t get_segment_capacity() const noexcept;

};

}}} // pma::adaptive::int1

#endif /* ADAPTIVE_INT1_PACKED_MEMORY_ARRAY_HPP_ */
