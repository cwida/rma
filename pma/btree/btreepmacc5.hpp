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

/**
 * As btreepmacc4, plus:
 * - Support for ::remove(int64_t) to delete elements
 * - Customise the min/max density thresholds (inherited from the class DensityBounds)
 */

#ifndef BTREEPMACC5_HPP_
#define BTREEPMACC5_HPP_

// gather rebalancing statistics
//#define PROFILING

#include "memory_pool.hpp"
#include "pma/bulk_loading.hpp"
#include "pma/density_bounds.hpp"
#include "pma/generic/static_index.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "timer.hpp"

namespace pma {

namespace btree_pmacc5_details {

/*****************************************************************************
 *                                                                           *
 *   Profiling                                                               *
 *                                                                           *
 *****************************************************************************/
class Instrumentation {
#if defined(PROFILING)
    // compiler barrier
    static inline void barrier(){ __asm__ __volatile__("": : :"memory"); };

    struct ProfileInfo{
        uint64_t m_time_total;  // total time, in microsecs
        uint64_t m_time_search; // search phase, in microsecs
        uint64_t m_time_operation; // spread/resize time, in microsecs
        uint32_t m_length; // window length in case of ::spread or new capacity in case of ::resize
        uint32_t m_previous; // 0 in case of ::spread and old capacity in case of ::resize;
        bool m_on_insert; // true if the rebalance occurred after an insert operation, false otherwise
    };

    // gathered profiles
    mutable std::vector<ProfileInfo> m_profiles;

public:
    class Profiler {
        const Instrumentation* m_base;
        Timer m_timer_total;
        Timer m_timer_search;
        Timer m_timer_operation; // either spread or resize
        uint32_t m_length = 0; // total number of segments, or new capacity in case of resizing
        uint32_t m_previous = 0; // previous capacity in case of resizing
        const bool m_on_insert;

    public:
        Profiler(Instrumentation* base, bool on_insert) : m_base(base), m_timer_total(true), m_on_insert(on_insert) { }

        ~Profiler();

        void search_start(){ barrier(); m_timer_search.start(); barrier(); }
        void search_stop() { barrier(); m_timer_search.stop(); barrier(); }

        void spread_start(size_t window_length){ barrier();  m_length = window_length; m_timer_operation.start(); barrier(); }
        void spread_stop(){ barrier(); m_timer_operation.stop(); barrier(); }

        void resize_start(size_t from, size_t to) { m_length = to; m_previous = from; m_timer_operation.start(); barrier(); }
        void resize_stop() { barrier(); m_timer_operation.stop(); }
    };

    Profiler profiler(bool is_insert) { return Profiler(this, is_insert); }



public:
    struct Statistics {
        uint64_t m_count =0; // number of elements counted
        uint64_t m_sum =0; // sum of the times, in microsecs
        uint64_t m_average =0; // average, in microsecs
        uint64_t m_min =0; // minimum, in microsecs
        uint64_t m_max =0; // maximum, in microsecs
        uint64_t m_stddev =0; // standard deviation, in microsecs
        uint64_t m_median =0; // median, in microsecs
    };

    struct StatisticsRebalances : public Statistics {
        uint64_t m_inserts;  // total number of inserts
        uint64_t m_deletes; // total number of deletes
    };

    struct CompleteStatistics {
        StatisticsRebalances m_cumulative; // total
        Statistics m_search; // search only
        std::vector<std::pair<uint32_t, Statistics>> m_spread; // invocations to ::spread
        std::vector<std::pair<uint32_t, Statistics>> m_resize_up; // increase the capacity
        std::vector<std::pair<uint32_t, Statistics>> m_resize_down; // halve the capacity
    };
    CompleteStatistics statistics() const;

#else
public:
    // dummy implementation
    struct Profiler {
        void search_start(){ }
        void search_stop() { }

        void spread_start(size_t) { }
        void spread_stop() { }

        void resize_start(size_t, size_t) { }
        void resize_stop() { }
    };
    Profiler profiler(bool) { return Profiler(); }
#endif
};

/*****************************************************************************
 *                                                                           *
 *   Internal PMA                                                            *
 *                                                                           *
 *****************************************************************************/

struct PMA {
    int64_t* m_keys; // pma for the keys
    int64_t* m_values; // pma for the values
    uint16_t* m_segment_sizes; // array, containing the cardinalities of each segment
    const uint16_t m_segment_capacity; // the max number of elements in a segment
    uint16_t m_height; // the height of the binary tree for elements
    uint32_t m_cardinality; // the number of elements contained
    uint32_t m_capacity; // the size of the array elements
    uint32_t m_number_segments; // the total number of segments, i.e. capacity / segment_size

    // Initialise the PMA for a given segment size
    PMA(size_t segment_size);

    void alloc_workspace(size_t num_segments, int64_t** keys, int64_t** values, decltype(m_segment_sizes)* sizes);
};

/*****************************************************************************
 *                                                                           *
 *   Internal iterator                                                       *
 *                                                                           *
 *****************************************************************************/
class Iterator : public pma::Iterator {
    const PMA& m_pma;
    size_t m_next_segment = 0;
    size_t m_offset = 0;
    size_t m_stop = 0; // index when the current sequence stops
    size_t m_index_max = 0;

    void next_sequence(); // update m_offset and m_stop to point to the next qualifying sequence

public:
    Iterator(const PMA& storage); // empty iterator
    Iterator(const PMA& storage, size_t segment_start, size_t segment_end, int64_t key_min, int64_t key_max);

    virtual bool hasNext() const;
    virtual std::pair<int64_t, int64_t> next();
};

/*****************************************************************************
 *                                                                           *
 *   Bulk loading metadata                                                   *
 *                                                                           *
 *****************************************************************************/
struct BlkRunInfo {
    uint64_t m_run_start; // start position in the sorted array for this run
    uint64_t m_run_length; // the number of elements of this run
    uint64_t m_cardinality; // the total cardinality = m_run_length + segment_sizes[i] /@ Range[i, m_segment_start, m_segment_start + m_segment_length -1]
    uint32_t m_segment_start; // the first segment associated to this run
    uint32_t m_segment_length; // the number of segments encompassed by this run
    bool m_valid; // whether this entry is valid or should be ignored in the merge

    /**
     * Create a new run entry with length =1
     * @param array_index the start position in the loaded array
     * @param segment_id the segment associated to this run
     */
    BlkRunInfo(uint64_t array_index, uint32_t segment_id);

};

using BlkRunAllocator = CachedAllocator<BlkRunInfo>;
using BlkRunVector = std::vector<BlkRunInfo, BlkRunAllocator>;

std::ostream& operator<<(std::ostream& out, const BlkRunInfo& entry);

} // namespace btree_pmacc5_details

class BTreePMACC5 : public InterfaceRQ, public SortedBulkLoading {
private:
    pma::StaticIndex m_index;
    btree_pmacc5_details::PMA m_storage;
    btree_pmacc5_details::Instrumentation m_instrumentation;
    CachedMemoryPool m_memory_pool;
    CachedDensityBounds m_density_bounds;
    bool m_segment_statistics = false; // record segment statistics at the end?

    // Insert an element in the given segment. It assumes that there is still room available
    // It returns true if the inserted key is the minimum in the interval
    bool storage_insert_unsafe(size_t segment_id, int64_t key, int64_t value);

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA, assuming the given bucket if it's not full.
    void insert_common(size_t segment_id, int64_t key, int64_t value);

    // Get the minimum of the given segment
    int64_t get_minimum(size_t segment_id) const;

    /**
     * Get the lower (out_a) and upper (out_b) threshold for the segments at the given `node_height'
     */
    std::pair<double, double> thresholds(int node_height);
    std::pair<double, double> thresholds(int node_height, int tree_height);

    /**
     * Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
     */
    void rebalance(size_t segment_id, int64_t* insert_new_key, int64_t* insert_new_value);

    /**
     * Resize the the capacity of the PMA and rebuild the underlying static index
     */
    void resize(int64_t* insert_new_key, int64_t* insert_new_value);

    /**
     * Spread the elements in the segments [segment_start, segment_start + num_segments)
     */
    struct spread_insert { int64_t m_key; int64_t m_value; size_t m_segment_id; };
    void spread(size_t cardinality, size_t segment_start, size_t num_segments, spread_insert* m_insertion);

    /**
     * Helper, copy the elements from <key_from,values_from> into the arrays <keys_to, values_to> and insert the new pair <key/value> in the sequence.
     */
    void spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value);

    /**
     * Returns an empty iterator, i.e. with an empty record set!
     */
    std::unique_ptr<pma::Iterator> empty_iterator() const;

    /**
     * Dump helpers
     */
    void dump_storage(std::ostream& out, bool* integrity_check) const;

    /**
     * Segment statistics
     */
    decltype(auto) compute_segment_statistics() const;
    void record_segment_statistics() const;

    /**
     * Rebalancing statistics
     */
#if defined(PROFILING)
    void record_rebalancing_statistics() const;
#endif

    /**
     * Bulk loading
     */
    btree_pmacc5_details::BlkRunVector load_generate_runs(std::pair<int64_t, int64_t>* array, size_t array_sz);
    bool load_fuse_runs(btree_pmacc5_details::BlkRunVector& runs);
    void load_spread(btree_pmacc5_details::BlkRunVector& runs);

    void load_spread(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz, const btree_pmacc5_details::BlkRunVector& runs);
    void load_merge_single(size_t segment_id, std::pair<int64_t, int64_t>* __restrict sequence, size_t sequence_sz, size_t cardinality);
    void load_merge_multi(size_t segment_start, size_t segment_length, std::pair<int64_t, int64_t>* __restrict sequence, size_t sequence_sz, size_t cardinality);
    void load_resize(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz);
    void load_empty(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz);
    void load_empty_single(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz);
    void load_empty_multi(std::pair<int64_t, int64_t>* __restrict array, size_t array_sz);

protected:
    /**
     * Bulk loading. Bottom up approach.
     */
    virtual void load_sorted(std::pair<int64_t, int64_t>* array, size_t array_sz) override;

public:
    BTreePMACC5();

    BTreePMACC5(size_t pma_segment_size);

    BTreePMACC5(size_t index_B, size_t pma_segment_size);

    virtual ~BTreePMACC5();

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

    virtual std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

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

    // Whether to save segment statistics, at the end, in the table `btree_leaf_statistics' ?
    void set_record_segment_statistics(bool value);
};

} // namespace pma

#endif /* BTREEPMACC5_HPP_ */
