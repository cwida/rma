/*
 * predictor.hpp
 *
 *  Created on: Jul 10, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef ADAPTIVE_BH07_V2_PREDICTOR_HPP_
#define ADAPTIVE_BH07_V2_PREDICTOR_HPP_

#include <cstddef>
#include <cinttypes>
#include <ostream>
#include <vector>

namespace pma { namespace adaptive { namespace bh07_v2 {

/**
 * An element in the predictor
 */
struct Item {
    uint64_t m_pointer : 48; // pointer to the key in APMA
    uint64_t m_count : 16; // count associated to the key
};

std::ostream& operator <<(std::ostream&, const Item&);

/**
 * An item in the predictor, paired with its position (permuted_position)
 */
struct PermutedItem {
    uint64_t m_pointer : 48; // pointer to the key in APMA, `int' to allow the special value -1 at the start of the array
    uint64_t m_count : 16; // count associated to the key
    uint64_t m_permuted_position; // the position of this item in the predictor buffer
};

std::ostream& operator<<(std::ostream&, const PermutedItem&);

class Predictor{
    Item* m_buffer; // underlying buffer
    size_t m_capacity; // capacity of the circular array
    size_t m_capacity_max; // capacity of the underlying buffer containing the circular array
    size_t m_tail; // pointer to the tail of the array
    size_t m_head; // pointer to the head of the array
    size_t m_count_max; // the max value a key in the array can hold
    bool m_empty; // is the circular buffer empty?

    /**
     * Allocate a buffer with the given capacity;
     */
    static Item* allocate_buffer(size_t capacity);

    /**
     * Deallocate the existing buffer
     */
    static void deallocate_buffer(Item*& buffer);

    /**
     * Find the position of the key in the circular array, or return -1 if not present
     */
    int64_t get_position(int64_t pointer) const;

    /**
     * Move the given item in the queue ahead of one position, if it's not already in the front
     * Return the new position of the item: either item_position +1, or 0 if it the item was at
     * the end of the circular array.
     */
    size_t move_ahead(size_t item_position);

    /**
     * Decrease the value for the element at the tail of the queue. If its count reaches 0, remove
     * the element from the queue, effectively moving the tail of one position ahead.
     */
    void decrease_tail();

    /**
     * Insert the key at the front of the queue, pointing to the cell in `pma_position' in the PMA.
     * As side effect, move the front of the queue ahead of one position.
     */
    void insert_new_element(uint64_t pointer);

    /**
     * Resize the underlying buffer with a new buffer of the given size. Copy all elements from the
     * old buffer to the new buffer, update the instance members to point to the new buffer.
     */
    void resize_with_new_buffer(size_t sz);

public:
    /**
     * Create a new instance with the given size
     */
    Predictor(size_t size, size_t max_count);

    /**
     * Destructor
     */
    ~Predictor();

    /**
     * Set the max value a key in the predictor can have
     */
    void set_max_count(size_t value);

    /**
     * Insert/Update the count for the given key in the predictor
     */
    void update(uint64_t pointer);

    /**
     * Alter the capacity of the underlying circular buffer
     */
    void resize(size_t sz);

    /**
     * Gets all elements in the predictor in the range [min, max]. The resulting
     * list is sorted by the key
     */
    std::vector<PermutedItem> weighted_elements(uint64_t min, uint64_t max);

    /**
     * Alias for the method #weighted_elements
     */
    std::vector<PermutedItem> items(uint64_t min, uint64_t max);

    /**
     * Reset the pointer for the element at the given position
     */
    void reset_ptr(size_t index, size_t pma_position);

    /**
     * Check whether the container is empty
     */
    bool empty() const;

    /**
     * Check whether the container is full
     */
    bool full() const;

    /**
     * Retrieve the number of elements stored
     */
    size_t size() const;

    /**
     * Dump the content of the data structure, for debug purposes.
     */
    void dump(std::ostream& out) const;
    void dump() const;
};

}}} // pma::adaptive::bh07_v2


#endif /* ADAPTIVE_BH07_V2_PREDICTOR_HPP_ */
