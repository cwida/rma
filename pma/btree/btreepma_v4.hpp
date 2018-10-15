/*
 * btreepma_v4.hpp
 *
 *  Created on: 9 Oct 2018
 *      Author: Dean De Leo
 */

#ifndef PMA_BTREE_BTREEPMA_V4_HPP_
#define PMA_BTREE_BTREEPMA_V4_HPP_

#include "pma/density_bounds.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"
#include "memory_pool.hpp"

#include <cinttypes>
#include <ostream>
#include <memory> // unique_ptr

namespace pma {

namespace btree_pma_v4_detail {

/*****************************************************************************
 *                                                                           *
 *   PMA elements (key/value pairs)                                          *
 *                                                                           *
 *****************************************************************************/
template<typename K, typename V>
struct PMA_Element {
    K key;
    V value;

    PMA_Element(): key(), value() { }
    PMA_Element(K& key, V& value) : key(key), value(value) { }
};

template<typename K, typename V>
std::ostream& operator<<(std::ostream& out, const PMA_Element<K, V>& element){
    out << "<" << element.key << ", " << element.value << ">";
    return out;
}

using element_t = PMA_Element<int64_t, int64_t>;

/*****************************************************************************
 *                                                                           *
 *   Internal PMA                                                            *
 *                                                                           *
 *****************************************************************************/
struct PMA {
    int64_t* m_keys; // pma for the keys
    int64_t* m_values; // pma for the values
    uint16_t* m_segment_cardinalities; // array, containing the cardinalities of each segment
    size_t m_capacity; // the size of the array elements
    size_t m_segment_capacity; // the max number of elements in a segment
    size_t m_number_segments; // the total number of segments, i.e. capacity / segment_size
    size_t m_height; // the height of the binary tree for elements
    size_t m_cardinality; // the number of elements contained
    const bool m_has_fixed_segment_capacity; // whether the max capacity of the segments is unaltered in resizes

    // Initialise the PMA for a given segment size
    PMA(size_t segment_capacity, bool has_fixed_segment_capacity);

    // Destructor
    ~PMA();

    // Allocate the arrays for the keys/values
    static void alloc_workspace(size_t num_segments, size_t segment_capacity, int64_t** keys, int64_t** values, decltype(m_segment_cardinalities)* cardinalities);
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


} // namespace btree_pma_v4_detail

/**
 * Clustered version of the baseline TPMA. The keys/values are split in separate arrays. Elements are packed at the extremes of a segments,
 * in the usual even/odd succession.
 */
class BTreePMA_v4 : public InterfaceRQ {
    void* m_index; // pimpl, dynamic (a,b)-tree
    btree_pma_v4_detail::PMA m_storage; // PMA
    CachedMemoryPool m_memory_pool;
    CachedDensityBounds m_density_bounds;

    /**
     * Get the lower (out_a) and upper (out_b) threshold for the segments at the given `node_height'
     */
    std::pair<double, double> thresholds(int node_height);
    std::pair<double, double> thresholds(int node_height, int tree_height);

    // Insert the first element in the (empty) container
    void insert_empty(int64_t key, int64_t value);

    // Insert an element in the PMA, assuming the given segment if it's not full.
    bool insert_common(uint64_t segment_id, int64_t key, int64_t value);

    // Find the segment having the greatest pivot that is less or equal than the given key
    uint64_t index_find_leq(int64_t key) const;

    // Find the segment having the smallest pivot that is greater or equal than the given key
    uint64_t index_find_geq(int64_t key) const;

    // Change the separator key for a given entry in the index
    void index_update(int64_t key_old, int64_t key_new);

    // Return the current minimum for the given segment
    int64_t get_minimum(uint64_t segment_id) const;

    // Retrieve the cardinality of the given segment
    uint64_t get_cardinality(uint64_t segment_id) const;

    // Attempt to rebalance the storage to ensure it stays in the targeted lower / upper bounds
    bool rebalance(uint64_t segment_id, int64_t new_key, int64_t new_value);

    // Spread the elements in the segments [window_start, window_start + num_segments)
    struct spread_insert { int64_t m_key; int64_t m_value; size_t m_segment_id; };
    void spread(size_t cardinality, size_t window_start, size_t num_segments, spread_insert* m_insertion);

    // Helper, copy the elements from <key_from,values_from> into the arrays <keys_to, values_to> and insert the new pair <key/value> in the sequence.
    void spread_insert_unsafe(int64_t* __restrict keys_from, int64_t* __restrict values_from, int64_t* __restrict keys_to, int64_t* __restrict values_to, size_t num_elements, int64_t new_key, int64_t new_value);

    // Resize the index, double the capacity of the PMA, rebuild the index
    void resize();

    // Returns an empty iterator, i.e. with an empty record set!
    std::unique_ptr<pma::Iterator> empty_iterator() const;

    // Dump the whole content of the PMA
    void dump_storage(std::ostream& out, bool* integrity_check) const;

public:
    /**
     * Constructor
     */
    BTreePMA_v4();

    /**
     * Constructor
     * @param segment_capacity: fix the segment capacity, in terms of number of elements, to the given value
     */
    BTreePMA_v4(uint64_t segment_capacity);

    /**
     * Destructor
     */
    virtual ~BTreePMA_v4();

    /**
     * Insert the given key/value
     */
    void insert(int64_t key, int64_t value) override;

    /**
     * Find the element with the given `key'. It returns its value if found, otherwise the value -1.
     * In case of duplicates, which element is returned is unspecified.
     */
    virtual int64_t find(int64_t key) const override;

    virtual std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;

    // Return an iterator over all elements of the PMA
    virtual std::unique_ptr<pma::Iterator> iterator() const override;

    // Sum all keys & values in the range [min, max]
    virtual SumResult sum(int64_t min, int64_t max) const override;

    // The number of elements stored
    virtual size_t size() const override;

    // Is this container empty?
    bool empty() const;

    // Current maximum capacity of a segment in the PMA, in terms of number of elements
    size_t segment_capacity() const;

    // Dump the content of the data structure to the given output stream (for debugging purposes)
    virtual void dump(std::ostream& out) const;

    // Dump the content of the data structure to stdout (for debugging purposes)
    virtual void dump() const override;
};

} /* namespace pma */

#endif /* PMA_BTREE_BTREEPMA_V4_HPP_ */
