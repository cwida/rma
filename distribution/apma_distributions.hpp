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
#ifndef APMA_DISTRIBUTIONS_HPP_
#define APMA_DISTRIBUTIONS_HPP_

#include "distribution.hpp"

#include <cmath>
#include <memory>
#include <vector>

#include "cbytearray.hpp"
#include "random_permutation.hpp"

namespace distribution {

/*****************************************************************************
 *                                                                           *
 *   SequentialForward                                                       *
 *                                                                           *
 *****************************************************************************/

/**
 * Generate the sequence [begin, begin +1, ..., end -2, end -1]
 */
class SequentialForward : public Distribution {
private:
    const int64_t m_begin; // inclusive
    const int64_t m_end; // exclusive

public:
    SequentialForward(int64_t start, int64_t end);

    size_t size() const override;

    int64_t key(size_t index) const override;

    std::unique_ptr<Distribution> view(size_t start, size_t length) override;

    bool is_dense() const override;
};


/*****************************************************************************
 *                                                                           *
 *   SequentialBackwards                                                     *
 *                                                                           *
 *****************************************************************************/

/**
 * Generate the sequence [end -1, end -2, ..., begin +1, begin].
 * This should simulate the proper "sequential" pattern defined in the APMA paper.
 */
class SequentialBackwards : public Distribution {
private:
    const int64_t m_begin; // inclusive
    const int64_t m_end; // exclusive

public:
    SequentialBackwards(int64_t start, int64_t end);

    size_t size() const override;

    int64_t key(size_t index) const override;

    std::unique_ptr<Distribution> view(size_t start, size_t length) override;

    bool is_dense() const override;
};


/*****************************************************************************
 *                                                                           *
 *   BulkDistribution                                                        *
 *                                                                           *
 *****************************************************************************/

/**
 * Bulk inserts with parameter 'alpha' in (0, 1]. The distribution works as follows:
 * - pick a random number and insert a sequential run of N * alpha
 * - then pick another random number and repeat the same process, until all elements have been examined
 * The final distribution, unless it is a view, is `dense' with no duplicates and no gaps.
 *
 * The template parameter T is the distribution of the sub-runs, either SequentialBackwards or SequentialForward
 */
template <typename T>
class BulkDistribution : public Distribution {
private:
    std::shared_ptr<T>* m_slots;
    size_t m_slots_sz;
    size_t m_keys_per_slot;
    size_t m_capacity;
    const size_t m_begin;
    const size_t m_end;
    const bool m_dense;

public:
    BulkDistribution<T>(size_t sz, double alpha);

    BulkDistribution<T>(const BulkDistribution<T>& copy, size_t begin, size_t end);

    ~BulkDistribution();

    size_t size() const override;

    int64_t key(size_t index) const override;

    std::unique_ptr<Distribution> view(size_t start, size_t length) override;

    bool is_dense() const override;
};

using BulkForward = BulkDistribution<SequentialForward>;
using BulkBackwards = BulkDistribution<SequentialBackwards>;

template<typename T>
BulkDistribution<T>::BulkDistribution(size_t sz, double alpha) : m_capacity(sz), m_begin(0), m_end(m_capacity), m_dense(true){
    using namespace std;

    if(sz == 0) throw std::invalid_argument("sz == 0");
    if(alpha <= 0 || alpha > 1) throw std::invalid_argument("invalid value for alpha, not in (0, 1]");
    m_keys_per_slot = min((size_t) ceil(pow(sz, alpha)), sz);
    m_slots_sz = sz / m_keys_per_slot +1; // +1 for the leftover
    size_t leftover = sz % m_keys_per_slot; // the size of the last slot

    RandomPermutationParallel rp(m_slots_sz, ARGREF(uint64_t, "seed_random_permutation") + 527021); // the first index is the position of the odd slot
    m_slots = new std::shared_ptr<T>[m_slots_sz]();
    size_t leftover_position = rp.get_raw_key(0);

    for(size_t i = 1; i < rp.size(); i++){
        size_t slot = rp.get_raw_key(i);
        size_t key_start = slot * m_keys_per_slot;
        if(slot > leftover_position) key_start += leftover - m_keys_per_slot;
        size_t key_end = key_start + m_keys_per_slot;

        m_slots[i -1] = make_shared<T>(key_start, key_end);
    }

    // remaining keys
    size_t key_start = leftover_position * m_keys_per_slot;
    size_t key_end = key_start + leftover;
    m_slots[m_slots_sz -1] = make_shared<T>(key_start, key_end);
}

template<typename T>
BulkDistribution<T>::BulkDistribution(const BulkDistribution& copy, size_t begin, size_t end) : m_slots_sz(copy.m_slots_sz), m_keys_per_slot(copy.m_keys_per_slot), m_capacity(copy.m_capacity), m_begin(begin), m_end(end), m_dense(false) {
    m_slots = new std::shared_ptr<T>[m_slots_sz]();
    for(size_t i = 0; i < m_slots_sz; i++){
        m_slots[i] = copy.m_slots[i];
    }
}

template<typename T>
BulkDistribution<T>::~BulkDistribution(){
    delete[] m_slots;
}

template<typename T>
size_t BulkDistribution<T>::size() const{
    return m_end - m_begin;
}

template<typename T>
int64_t BulkDistribution<T>::key(size_t index) const {
    index += m_begin; // in case this is a view, shift to right

    size_t base = index / m_keys_per_slot;
    size_t offset = index % m_keys_per_slot;
    return m_slots[base]->key(offset) +1; //+1 to have keys in [1, N] rather than [0, N)
}

template<typename T>
std::unique_ptr<Distribution> BulkDistribution<T>::view(size_t start, size_t length) {
    if(start + length > m_capacity) throw std::invalid_argument("End interval out of bound");
    return std::unique_ptr<Distribution>{new BulkDistribution<T>(*this, start, start + length)};
}

template<typename T>
bool BulkDistribution<T>::is_dense() const {
    return m_dense;
}

/*****************************************************************************
 *                                                                           *
 *   InterleavedDistribution                                                 *
 *                                                                           *
 *****************************************************************************/

/**
 * Create 'num_streams' sequential runs. The generated elements are interleaved
 * among each run, e.g.
 * 0 -> Run 0, element 0
 * 1 -> Run 1, element 0
 * ...
 * N-1 -> Run N-1, element 0
 * N -> Run 0, element 1
 * N +1 _> Run 1, element 1
 * ...
 *
 * The template parameter T is the distribution of the sub-runs, either SequentialBackwards or SequentialForward
 */
template <typename T>
class InterleavedDistribution : public Distribution {
private:
    std::shared_ptr<T>* m_slots;
    size_t m_slots_sz;
    size_t m_capacity;
    const size_t m_begin;
    const size_t m_end;
    const bool m_dense;

public:
    InterleavedDistribution(size_t size, size_t num_streams);

    InterleavedDistribution(const InterleavedDistribution& copy, size_t begin, size_t end);

    ~InterleavedDistribution();

    size_t size() const override;

    int64_t key(size_t index) const override;

    std::unique_ptr<Distribution> view(size_t start, size_t length) override;

    bool is_dense() const override;
};

using InterleavedForward = InterleavedDistribution<SequentialForward>;
using InterleavedBackwards = InterleavedDistribution<SequentialBackwards>;

template <typename T>
InterleavedDistribution<T>::InterleavedDistribution(size_t size, size_t num_streams) : m_capacity(size), m_begin(0), m_end(m_capacity), m_dense(true){
    if(size == 0) throw std::invalid_argument("size == 0");
    if(num_streams == 0 || num_streams > size) throw std::invalid_argument("invalid value for `num_stream' (alpha), not in (0, size)");
    size_t keys_per_slot = size / num_streams;
    m_slots_sz = num_streams;
    size_t leftover_index = size % num_streams; // the first `leftover_index' will have an element less

    RandomPermutationParallel rp(m_slots_sz, ARGREF(uint64_t, "seed_random_permutation") + 527021);
    m_slots = new std::shared_ptr<T>[m_slots_sz]();

    std::unique_ptr<int[]> permutations{ new int[m_slots_sz] };

    for(size_t i = 0; i < m_slots_sz; i++){
        size_t slot = rp.get_raw_key(i);
        permutations[slot] = i;
    }

    size_t start = 0;
    for(size_t i = 0; i < m_slots_sz; i++){
        size_t j = permutations[i];
        size_t length = keys_per_slot + (j < leftover_index ? 1 : 0);
        size_t end = start + length;
        m_slots[j] = std::make_shared<T>(start, end);

        start = end; // next iteration
    }
}

template <typename T>
InterleavedDistribution<T>::InterleavedDistribution(const InterleavedDistribution& copy, size_t begin, size_t end) : m_slots_sz(copy.m_slots_sz), m_capacity(copy.m_capacity), m_begin(begin), m_end(end), m_dense(false) {
    m_slots = new std::shared_ptr<T>[m_slots_sz]();
    for(size_t i = 0; i < m_slots_sz; i++){
        m_slots[i] = copy.m_slots[i];
    }
}

template <typename T>
InterleavedDistribution<T>::~InterleavedDistribution(){
    delete[] m_slots;
}

template <typename T>
size_t InterleavedDistribution<T>::size() const  {
    return m_end - m_begin;
}

template <typename T>
int64_t InterleavedDistribution<T>::key(size_t index) const {
    index += m_begin; // in case this is a view, shift to right

    size_t base = index % m_slots_sz;
    size_t offset = index / m_slots_sz;
    return m_slots[base]->key(offset)  +1; //+1 to have keys in [1, N] rather than [0, N)
}

template <typename T>
std::unique_ptr<Distribution> InterleavedDistribution<T>::view(size_t start, size_t length) {
    if(start + length > m_capacity) throw std::invalid_argument("End interval out of bound");
    return std::unique_ptr<Distribution>{new InterleavedDistribution(*this, start, start + length)};
}

template <typename T>
bool InterleavedDistribution<T>::is_dense() const {
    return m_dense;
}

/*****************************************************************************
 *                                                                           *
 *   NoiseDistribution                                                       *
 *                                                                           *
 *****************************************************************************/
/**
 * Insert up to `N * alpha' following the sequential pattern at the start of the
 * array, and the rest uniformly, randomly interleaving between the sequential pattern
 * and the uniform distribution.
 * The distribution has the purpose of simulating noise in a random pattern.
 *
 *
 * The template parameter T is the distribution of the suquential pattern, either
 * SequentialBackwards or SequentialForward.
 */
template<typename T>
class NoiseDistribution : public Distribution {
private:
    std::shared_ptr<T> m_sequential;
    std::shared_ptr<CByteArray> m_permutation_ptr;
    CByteArray* m_permutation; // avoid the overhead of the shared_ptr
    size_t m_threshold; // the keys from [0, m_threshold) follow the sequential pattern, the rest is uniformly distributed
    size_t m_capacity;
    const size_t m_begin;
    const size_t m_end;
    const bool m_dense;

public:
    NoiseDistribution(size_t size, double alpha);

    NoiseDistribution(const NoiseDistribution& copy, size_t begin, size_t end);

    ~NoiseDistribution();

    size_t size() const override;

    int64_t key(size_t index) const override;

    std::unique_ptr<Distribution> view(size_t start, size_t length) override;

    bool is_dense() const override;
};

using NoiseForward = NoiseDistribution<SequentialForward>;
using NoiseBackwards = NoiseDistribution<SequentialBackwards>;

template <typename T>
NoiseDistribution<T>::NoiseDistribution(size_t size, double alpha) : m_capacity(size), m_begin(0), m_end(m_capacity), m_dense(true){
    if(size == 0) throw std::invalid_argument("size == 0");
    if(alpha <= 0 || alpha >= 1) throw std::invalid_argument("invalid value for `alpha', not in (0, 1)");
    m_threshold = alpha * m_capacity;
    m_sequential = std::make_shared<T>(1, m_threshold +1);

    // permute the elements
    RandomPermutationParallel rp(m_capacity, ARGREF(uint64_t, "seed_random_permutation") + 527021);
    m_permutation_ptr = rp.get_container();
    m_permutation = m_permutation_ptr.get();

    // reorder all keys with value in [0, m_threshold) so that they are sequential
    std::unique_ptr<int[]> reordering{ new int[m_threshold] };

    for(size_t i = 0, j = 0; i < m_capacity; i++){
        size_t slot = m_permutation->get_value_at(i);
        if(slot < m_threshold){
            reordering[j++] = i;
        }
    }
    for(size_t i = 0; i < m_threshold; i++){
        size_t j = reordering[i];
        m_permutation->set_value_at(j, i);
    }

}

template <typename T>
NoiseDistribution<T>::NoiseDistribution(const NoiseDistribution& copy, size_t begin, size_t end) :
        m_sequential(copy.m_sequential), m_permutation_ptr(copy.m_permutation_ptr), m_permutation(m_permutation_ptr.get()),
        m_threshold(copy.m_threshold), m_capacity(copy.m_capacity), m_begin(begin), m_end(end), m_dense(false) {

}

template <typename T>
NoiseDistribution<T>::~NoiseDistribution(){
    // !!! DO NOT DELETE[] this->permutation, already wrapped in a shared_ptr !!!
    m_permutation = nullptr;
}

template <typename T>
size_t NoiseDistribution<T>::size() const  {
    return m_end - m_begin;
}

template <typename T>
int64_t NoiseDistribution<T>::key(size_t index) const {
    index += m_begin;

    size_t offset = m_permutation->get_value_at(index);
    if(offset < m_threshold)
        return m_sequential->key(offset);
    else
        return offset +1;  // +1 to have keys in [1, N] rather than [0, N)
}

template <typename T>
std::unique_ptr<Distribution> NoiseDistribution<T>::view(size_t start, size_t length) {
    if(start + length > m_capacity) throw std::invalid_argument("End interval out of bound");
    return std::unique_ptr<Distribution>{new NoiseDistribution(*this, start, start + length)};
}

template <typename T>
bool NoiseDistribution<T>::is_dense() const {
    return m_dense;
}


} // namespace distribution

#endif /* APMA_DISTRIBUTIONS_HPP_ */
