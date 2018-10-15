/*
 * Wrapper for the PMA implementation used in the paper:
 * M. Durand, B. Raffin, F. Faure, A Packed Memory Array to Keep Moving Particles, VRIPHYS 2012
 *
 * The source code of the actual implementation has been provided by the authors
 * of the paper under GPL v3 terms.
 */

#ifndef EXTERNAL_DFR_DFR_HPP_
#define EXTERNAL_DFR_DFR_HPP_

#include "pma/bulk_loading.hpp"
#include "pma/interface.hpp"
#include "pma/iterator.hpp"

namespace pma { namespace dfr {

/**
 * Wrapper to the DFR implementation, exposing the common interface for the experiments
 */
class PackedMemoryArray : public pma::InterfaceRQ, public pma::SortedBulkLoading {
    void* m_handle; // opaque handle to the PMA implementation

    // Find the start & end indices for the interval [min, max]
    std::pair<int64_t, int64_t> find_interval(int64_t min, int64_t max) const;

    // Retrieve the slot at the given position
    std::pair<int64_t, int64_t>& get(uint64_t i) const;

protected:
    void load_sorted(std::pair<int64_t, int64_t>* array, size_t array_sz) override;

public:
    /**
     * Initialise the data structure using the defaults from the DFR implementation
     */
    PackedMemoryArray();

    /**
     * Initialise the data structure with the given parameters
     * @param tau_0 upper threshold for the leaves of the calibrator tree
     * @param tau_h upper threshold for the root of the calibrator tree
     * @param rho_0 lower threshold for the root of the calibrator tree
     * @param rho_h lower threshold for the leaves of the calibrator tree
     * @param segment_size the size of each segment in the PMA
     */
    PackedMemoryArray(double tau_0, double tau_h, double rho_0, double rho_h, unsigned int segment_size);

    /**
     * Destructor
     */
    ~PackedMemoryArray();

    void insert(int64_t key, int64_t value) override;
    int64_t find(int64_t key) const override;
    std::size_t size() const override;
    std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;
    std::unique_ptr<pma::Iterator> iterator() const override;
    pma::Interface::SumResult sum(int64_t min, int64_t max) const override;
    void dump() const override;
};


}} // pma::dfr

#endif /* EXTERNAL_DFR_DFR_HPP_ */
