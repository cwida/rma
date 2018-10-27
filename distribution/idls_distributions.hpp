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

#ifndef IDLS_DISTRIBUTIONS_HPP_
#define IDLS_DISTRIBUTIONS_HPP_

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace distribution { namespace idls {

/**
* Forward declarations
*/
class Generator;

/**
 * The kind of distribution to use
 */
enum class eDistributionType { uniform, zipf, sequential };

/**
 * Directive to perform a single scan from `key_min' to `key_max'. The sum
 * of the keys is expected to be `expected_sum'
 */
struct ScanRange {
    int64_t key_min; // start of the interval, inclusive
    int64_t key_max; // end of the interval, inclusive
    uint64_t expected_sum_keys; // the expected sum of all keys in [key_min, key_max]
    uint64_t expected_sum_values; // the expected sum of all keys in [key_min, key_max]
};

/**
 * Java-style iterator. Generate one key at the time, of type T.
 */
template <typename T>
class Distribution {
public:
    /**
     * Virtual destructor
     */
    virtual ~Distribution() { };

    /**
     * Check whether a new key can be generated
     */
    virtual bool hasNext() const = 0;

    /**
     * Get the next key in the container
     */
    virtual T next() = 0;
};

/**
 * The distributions required in the idls experiment
 */
class DistributionsContainer {
    friend class Generator;
    std::shared_ptr<std::vector<int64_t>> m_keys_initial_size; // keys for the initial step
    std::shared_ptr<std::vector<int64_t>> m_keys_insdel; // keys for the insert/delete step
    std::shared_ptr<std::vector<int64_t>> m_keys_lookup; // keys for the lookup step
    std::vector<std::pair<double, std::shared_ptr<std::vector<ScanRange>>>> m_keys_scan; // intervals for the scan step

public:
    /**
     * Empty constructor
     */
    DistributionsContainer();

    /**
     * Iterator for the preparation step. Insert the tuples <k, k>, where k is the extracted key
     */
    std::unique_ptr<Distribution<int64_t>> preparation_step();

    /**
     * Unset the keys stored for the preparation step (to free some memory...)
     */
    void unset_preparation_step();

    /**
     * Iterator for the insert/delete step. If the key is positive, insert the tuple <k, k>.
     * Otherwise the extracted key k is negative, remove from the PMA switching its sign, i.e. -k.
     */
    std::unique_ptr<Distribution<int64_t>> insdel_step();


    /**
     * Unset the keys stored for the insert/delete step (to free some memory...)
     */
    void unset_insdel_step();

    /**
     * Iterator for the lookup step.
     */
    std::unique_ptr<Distribution<int64_t>> lookup_step();

    /**
     * Iterator for the scan step. Retrieve a vector of
     */
    std::vector<std::pair<double, std::unique_ptr<Distribution<ScanRange>>>> scan_step();

};

/**
 * Create the distributions for the IDLS experiment
 */
class Generator {
    size_t m_num_initial_inserts = 0; // the number of insertions in the initial step
    size_t m_num_ins_del = 0; // total number of insert/deletes in the second step
    size_t m_num_lookups = 0; // total number of lookups
    std::vector<std::pair<double, size_t>> m_num_scans; // a vector of scans <interval_size in (0, 1], num repetitions>

    size_t m_num_insertions_per_batch =1; // number of consecutive insertions per group
    size_t m_num_deletions_per_batch =1; // number of consecutive deletions per group

    eDistributionType m_init_distribution_type; // the distribution to employ to insert the first `m_num_initial_inserts' elements
    double m_init_alpha = 0; // distribution parameter (zipf factor)
    eDistributionType m_insert_distribution_type; // the distribution to employ to generate the insertions
    double m_insert_alpha = 0; // distribution parameter (zipf factor)
    eDistributionType m_delete_distribution_type; // the distribution to employ to generate the deletions
    double m_delete_alpha = 0; // distribution parameter (zipf factor)
    size_t m_distribution_range = 0; // the range for the distributions (param beta shared among all distributions)
    uint64_t m_seed = 1; // random seed
    bool m_restore_initial_size = true; // after all batches of insertions/deletions have been executed, does it need to create a final batch to
                                        // bring back the final cardinality of the data structure to its initial size?

public:
    Generator();

    // Set the number of inserts for initial size
    void set_initial_size(size_t value);

    // Set the total number of insert/deletes. The param `group_size' determines how many consecutive inserts/deletes to perform
    // before switching to the next operation
    void set_insdel(size_t total, size_t group_size);

    // Set the total number of lookups
    void set_lookups(size_t total);

    // Set the intervals for the scans. The first value of the pair is the interval size, in (0, 1]. The second value is
    // the number of scans to repeat for the associated interval size.
    void set_scans(std::vector<std::pair<double, size_t>>& scans);

    // Set the random seed
    void set_seed(uint64_t random_seed);

    // Set the distribution type
    void set_distribution_type_init(eDistributionType value, double alpha);
    void set_distribution_type_insert(eDistributionType value, double alpha);
    void set_distribution_type_delete(eDistributionType value, double alpha);
    void set_distribution_range(size_t range);
    void set_restore_initial_size(bool value);

    DistributionsContainer generate() const;
};


}} // namespace distribution::idls


#endif /* IDLS_DISTRIBUTIONS_HPP_ */
