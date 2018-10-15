/*
 * packed_memory_array.hpp
 *
 *  Created on: 9 Sep 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_ADAPTIVE_INT3_PACKED_MEMORY_ARRAY_HPP_
#define PMA_ADAPTIVE_INT3_PACKED_MEMORY_ARRAY_HPP_

#include <memory>
#include <random>

#include "pma/density_bounds.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "detector.hpp"
#include "knobs.hpp"
#include "memory_pool.hpp"
#include "partition.hpp"
#include "rebalance_metadata.hpp"
#include "static_abtree.hpp"
#include "storage.hpp"

namespace pma { namespace adaptive { namespace int3 {

class SpreadWithRewiring; // forward decl.
class Weights;

class PackedMemoryArray : public InterfaceRQ {
    friend class SpreadWithRewiring;
    friend class Weights;
private:
    StaticABTree m_index;
    Storage m_storage;
    Knobs m_knobs; // APMA settings
    Detector m_detector;
    CachedDensityBounds m_density_bounds0; // user thresholds (for num_segments<=balanced_thresholds_cutoff())
    CachedDensityBounds m_density_bounds1; // primary thresholds (for num_segmnets>balanced_thresholds_cutoff())
    CachedMemoryPool m_memory_pool;
    bool m_segment_statistics = false; // record segment statistics at the end?
    bool m_primary_densities = false; // use the primary thresholds?

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA, assuming the given bucket if it's not full.
    void insert_common(size_t segment_id, int64_t key, int64_t value);

    // Determine the window to rebalance
    void rebalance_find_window(size_t segment_id, bool is_insert, int64_t* out_window_start, int64_t* out_window_length, int64_t* out_cardinality_after, bool* out_resize) const;

    // Determine whether to rebalance or resize the underlying storage
    RebalanceMetadata rebalance_plan(bool is_insert, int64_t window_start, int64_t window_length, int64_t cardinality_after, bool resize) const;

    // Determine the size of each partition according to the APMA algorithm
    void rebalance_run_apma(RebalanceMetadata& md);

    // Rebalance the storage so that the density thresholds are ensured
    void rebalance(size_t segment_id, int64_t* insert_new_key, int64_t* insert_new_value);

    // Perform the rebalancing action
    void do_rebalance(const RebalanceMetadata& action);

    // Retrieve the lower & higher thresholds of the calibrator tree
    std::pair<double, double> get_thresholds(int height) const;

    // Reset the thresholds for the calibrator tree
    void set_thresholds(int height_calibrator_tree);

    // Update the thresholds for the calibrator tree
    void set_thresholds(const RebalanceMetadata& md);

    // Rebuild the underlying storage to hold m_elements
    void resize(const RebalanceMetadata& action);

    // Spread (with rewiring) the elements in the given window
    void resize_rebalance(const RebalanceMetadata& action);

    // Spread (without rewiring) the elements in the given window
    void spread_local(const RebalanceMetadata& action);
    struct spread_detector_record{ int64_t m_position; int64_t m_predecessor; int64_t m_successor; };
    void spread_load(const RebalanceMetadata& action, int64_t* __restrict keys_to, int64_t* __restrict values_to, int64_t* out_insert_position = nullptr);
    void spread_save(size_t segment_id, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record);
    void spread_save(size_t segment_start, size_t segment_length, int64_t* keys_from, int64_t* values_from, size_t cardinality, const spread_detector_record* detector_record);
    size_t spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value);

    // Helper, create the data structure detector record, based on the position the new key has been inserted
    spread_detector_record spread_create_detector_record(int64_t* keys, int64_t size, int64_t position);

    // Dump the content of the storage
    void dump_storage(std::ostream& out, bool* integrity_check) const;

    // Segment statistics
    decltype(auto) compute_segment_statistics() const;
    void record_segment_statistics() const;

    // Retrieve the number of segments after that the primary thresholds are used
    size_t balanced_thresholds_cutoff() const;

    // Returns an empty iterator, i.e. with an empty record set!
    std::unique_ptr<pma::Iterator> empty_iterator() const;

protected:
    // Helper for the class Weights
    // Find the position of the key in the given segment, or return -1 if not found.
    int find_position(size_t segment_id, int64_t key) const noexcept;

public:
    PackedMemoryArray(size_t pages_per_extent);

    PackedMemoryArray(size_t pma_segment_size, size_t pages_per_extent);

    PackedMemoryArray(size_t index_B, size_t pma_segment_size, size_t pages_per_extent);

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

    // Sum all elements in the interval [min, max]
    virtual ::pma::Interface::SumResult sum(int64_t min, int64_t max) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<::pma::Iterator> iterator() const override;

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

    // Retrieve the densities currently in use
    const CachedDensityBounds& get_thresholds() const;

    // Accessor to the APMA settings
    Knobs& knobs();

    // Accessor, the max capacity of each segment
    size_t get_segment_capacity() const noexcept;

    // Memory footprint
    size_t memory_footprint() const override;
};

// Dump
std::ostream& operator<<(std::ostream& out, const PackedMemoryArray& pma);

}}} // pma::adaptive::int3

#endif /* PMA_ADAPTIVE_INT3_PACKED_MEMORY_ARRAY_HPP_ */
