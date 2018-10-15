/*
 * bulk_loading.hpp
 *
 *  Created on: May 15, 2018
 *      Author: dleo@cwi.nl
 */

#ifndef PMA_BULK_LOADING_HPP_
#define PMA_BULK_LOADING_HPP_

#include <cstddef>
#include <cstdint>
#include <utility>

namespace pma {

/**
 * Interface to load batch of elements in the container
 */
class BulkLoading {
public:
    /**
     * Public destructor
     */
    virtual ~BulkLoading();

    /**
     * Load the given array of pairs key/value
     * @param array the elements to load. The array is not guaranteed to remain constant. The memory
     *      allocation of the array must be managed by the caller (malloc/free).
     * @param array_sz the number of elements of array.
     */
    virtual void load(std::pair<int64_t, int64_t>* array, size_t array_sz) = 0;
};


/**
 * Convenience class for bulk loading. It sorts the input elements and pass the array
 * to the interface implementation
 */
class SortedBulkLoading : public BulkLoading {
protected:
    /**
     * Actual implementation of bulk loading. The input array is sorted.
     */
    virtual void load_sorted(std::pair<int64_t, int64_t>* array, size_t array_sz) = 0;

public:
    /**
     * Loads the given array of pairs key/value. It sorts the elements in the array.
     * @param array the elements to load. The array is not guaranteed to remain constant. The memory
     *      allocation of the array must be managed by the caller (malloc/free).
     * @param array_sz the number of elements of array.
     */
    void load(std::pair<int64_t, int64_t>* array, size_t array_sz) override;
};

} // namespace pma
#endif /* PMA_BULK_LOADING_HPP_ */
