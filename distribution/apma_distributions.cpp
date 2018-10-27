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

#include "apma_distributions.hpp"

#include <cmath>
#include <stdexcept>

#include "console_arguments.hpp"
#include "random_permutation.hpp"

using namespace std;

namespace distribution {

/*****************************************************************************
 *                                                                           *
 *   SequentialForward                                                       *
 *                                                                           *
 *****************************************************************************/

SequentialForward::SequentialForward(int64_t start, int64_t end): m_begin(start), m_end(end) {
    if(start > end) throw std::invalid_argument("start > end");
}
size_t SequentialForward::size() const { return m_end - m_begin; }
int64_t SequentialForward::key(size_t offset) const {
    assert(offset < (m_end - m_begin));
    return m_begin + offset;
}
unique_ptr<Distribution> SequentialForward::view(size_t start, size_t length) { return make_unique<SequentialForward>(start, start + length); }
bool SequentialForward::is_dense() const { return true; }

/*****************************************************************************
 *                                                                           *
 *   SequentialBackwards                                                     *
 *                                                                           *
 *****************************************************************************/
SequentialBackwards::SequentialBackwards(int64_t start, int64_t end): m_begin(start), m_end(end){
    if(start > end) throw std::invalid_argument("start > end");
}
size_t SequentialBackwards::size() const { return m_end - m_begin; }
int64_t SequentialBackwards::key(size_t offset) const { return m_end -1 - offset; }
unique_ptr<Distribution> SequentialBackwards::view(size_t start, size_t length) { return make_unique<SequentialBackwards>(start, start + length); }
bool SequentialBackwards::is_dense() const { return true; }

/*****************************************************************************
 *                                                                           *
 *   BulkForward                                                             *
 *                                                                           *
 *****************************************************************************/
//class BulkForward : public Distribution {
//private:
//    std::shared_ptr<SequentialForward>* m_slots;
//    size_t m_slots_sz;
//    size_t m_keys_per_slot;
//    size_t m_capacity;
//    const size_t m_begin;
//    const size_t m_end;
//    const bool m_dense;
//
//public:
//    BulkForward(size_t sz, double alpha);
//
//    BulkForward(BulkForward& copy, size_t begin, size_t end);
//
//    ~BulkForward();
//
//    size_t size() const override;
//
//    int64_t key(size_t index) const override;
//
//    std::unique_ptr<Distribution> view(size_t start, size_t length) override;
//
//    bool is_dense() const override;
//};
//
//BulkForward::BulkForward(size_t sz, double alpha) : m_capacity(sz), m_begin(0), m_end(m_capacity), m_dense(true){
//    if(sz == 0) throw std::invalid_argument("sz == 0");
//    if(alpha <= 0 || alpha > 1) throw std::invalid_argument("invalid value for alpha, not in (0, 1]");
//    m_keys_per_slot = min((size_t) ceil(sz * alpha), sz);
//    m_slots_sz = sz / m_keys_per_slot +1; // +1 for the leftover
//    cout << "sz: " << sz << ", number of slots: " << m_slots_sz << ", slot_size_regular: " << m_keys_per_slot << "\n";
//    size_t leftover = sz % m_keys_per_slot; // the size of the last slot
//
//    RandomPermutationParallel rp(m_slots_sz, ARGREF(uint64_t, "seed_random_permutation") + 527021); // the first index is the position of the odd slot
//    m_slots = new std::shared_ptr<SequentialForward>[m_slots_sz]();
//    size_t leftover_position = rp.get_raw_key(0);
//    cout << "odd_slot_position: " << leftover_position << ", size: " << leftover << endl;
//
//    for(size_t i = 1; i < rp.size(); i++){
//        size_t slot = rp.get_raw_key(i);
//        size_t key_start = slot * m_keys_per_slot;
//        if(slot > leftover_position) key_start += leftover - m_keys_per_slot;
//        size_t key_end = key_start + m_keys_per_slot;
//
//        cout << "[" << i -1 << "] slot: " << slot << ", start: " << key_start << ", end: " << key_end << endl;
//        m_slots[i -1] = make_shared<SequentialForward>(key_start, key_end);
//    }
//
//    // remaining keys
//    size_t key_start = leftover_position * m_keys_per_slot;
//    size_t key_end = key_start + leftover;
//    cout << "[" << m_slots_sz -1 << "] slot: " << leftover_position << ", start: " << key_start << ", end: " << key_end << endl;
//    m_slots[m_slots_sz -1] = make_shared<SequentialForward>(key_start, key_end);
//}
//
//BulkForward::BulkForward(BulkForward& copy, size_t begin, size_t end) : m_slots_sz(copy.m_slots_sz), m_keys_per_slot(copy.m_keys_per_slot), m_capacity(copy.m_capacity), m_begin(begin), m_end(end), m_dense(false) {
//    m_slots = new std::shared_ptr<SequentialForward>[m_slots_sz]();
//    for(size_t i = 0; i < m_slots_sz; i++){
//        m_slots[i] = copy.m_slots[i];
//    }
//}
//
//BulkForward::~BulkForward(){
//    delete[] m_slots;
//}
//
//size_t BulkForward::size() const  {
//    return m_end - m_begin;
//}
//
//int64_t BulkForward::key(size_t index) const {
//    index += m_begin; // in case this is a view, shift to right
//
//    size_t base = index / m_keys_per_slot;
//    size_t offset = index % m_keys_per_slot;
//    return m_slots[base]->key(offset);
//}
//
//unique_ptr<Distribution> BulkForward::view(size_t start, size_t length) {
//    if(start + length > m_capacity) throw std::invalid_argument("End interval out of bound");
//    return unique_ptr<Distribution>{new BulkForward(*this, start, start + length)};
//}
//
//bool BulkForward::is_dense() const {
//    return m_dense;
//}


/*****************************************************************************
 *                                                                           *
 *   MultipleSequential                                                      *
 *                                                                           *
 *****************************************************************************/

//class MultipleSequentialDistribution : public Distribution {
//private:
//    std::shared_ptr<SequentialForward>* m_slots;
//    size_t m_slots_sz;
//    size_t m_capacity;
//    const size_t m_begin;
//    const size_t m_end;
//    const bool m_dense;
//
//public:
//    MultipleSequentialDistribution(size_t size, size_t num_streams);
//
//    MultipleSequentialDistribution(MultipleSequentialDistribution& copy, size_t begin, size_t end);
//
//    ~MultipleSequentialDistribution();
//
//    size_t size() const override;
//
//    int64_t key(size_t index) const override;
//
//    std::unique_ptr<Distribution> view(size_t start, size_t length) override;
//
//    bool is_dense() const override;
//};
//
//MultipleSequentialDistribution::MultipleSequentialDistribution(size_t size, size_t num_streams) : m_capacity(size), m_begin(0), m_end(m_capacity), m_dense(true){
//    if(size == 0) throw std::invalid_argument("size == 0");
//    if(num_streams == 0 || num_streams > size) throw std::invalid_argument("invalid value for `num_stream' (alpha), not in (0, size)");
//    size_t keys_per_slot = size / num_streams;
//    m_slots_sz = num_streams;
//    size_t leftover_index = size % num_streams; // the first `leftover_index' will have an element less
//    cout << "sz: " << size << ", number of slots: " << m_slots_sz << ", keys per slot: " << keys_per_slot << ", leftover: " << leftover_index << "\n";
//
//    RandomPermutationParallel rp(m_slots_sz, ARGREF(uint64_t, "seed_random_permutation") + 527021);
//    m_slots = new std::shared_ptr<SequentialForward>[m_slots_sz]();
//
//    unique_ptr<int[]> permutations{ new int[m_slots_sz] };
//
//    for(size_t i = 0; i < m_slots_sz; i++){
//        size_t slot = rp.get_raw_key(i);
//        permutations[slot] = i;
//    }
//
//    size_t start = 0;
//    for(size_t i = 0; i < m_slots_sz; i++){
//        size_t j = permutations[i];
//        size_t length = keys_per_slot + (j < leftover_index ? 1 : 0);
//        size_t end = start + length;
//        cout << "[" << j << "] start: " << start << ", end: " << end << endl;
//        m_slots[j] = make_shared<SequentialForward>(start, end);
//
//        start = end; // next iteration
//    }
//}
//
//MultipleSequentialDistribution::MultipleSequentialDistribution(MultipleSequentialDistribution& copy, size_t begin, size_t end) : m_slots_sz(copy.m_slots_sz), m_capacity(copy.m_capacity), m_begin(begin), m_end(end), m_dense(false) {
//    m_slots = new std::shared_ptr<SequentialForward>[m_slots_sz]();
//    for(size_t i = 0; i < m_slots_sz; i++){
//        m_slots[i] = copy.m_slots[i];
//    }
//}
//
//MultipleSequentialDistribution::~MultipleSequentialDistribution(){
//    delete[] m_slots;
//}
//
//size_t MultipleSequentialDistribution::size() const  {
//    return m_end - m_begin;
//}
//
//int64_t MultipleSequentialDistribution::key(size_t index) const {
//    index += m_begin; // in case this is a view, shift to right
//
//    size_t base = index % m_slots_sz;
//    size_t offset = index / m_slots_sz;
//    return m_slots[base]->key(offset)  +1; //+1 to have keys in [1, N] rather than [0, N)
//}
//
//unique_ptr<Distribution> MultipleSequentialDistribution::view(size_t start, size_t length) {
//    if(start + length > m_capacity) throw std::invalid_argument("End interval out of bound");
//    return unique_ptr<Distribution>{new MultipleSequentialDistribution(*this, start, start + length)};
//}
//
//bool MultipleSequentialDistribution::is_dense() const {
//    return m_dense;
//}

/*****************************************************************************
 *                                                                           *
 *   NoiseDistribution                                                       *
 *                                                                           *
 *****************************************************************************/

//NoiseDistribution::NoiseDistribution(size_t size, double alpha) : m_capacity(size), m_begin(0), m_end(m_capacity), m_dense(true){
//    if(size == 0) throw std::invalid_argument("size == 0");
//    if(alpha <= 0 || alpha >= 1) throw std::invalid_argument("invalid value for `alpha', not in (0, 1)");
//    m_threshold = alpha * m_capacity;
//    m_sequential = make_shared<SequentialForward>(1, m_threshold +1);
//    cout << "size: " << size << ", threshold: " << m_threshold << endl;
//
//    // permute the elements
//    RandomPermutationParallel rp(m_capacity, ARGREF(uint64_t, "seed_random_permutation") + 527021);
//    m_permutation_ptr = rp.get_container();
//    m_permutation = m_permutation_ptr.get();
//
//    // reorder all keys with value in [0, m_threshold) so that they are sequential
//    unique_ptr<int[]> reordering{ new int[m_threshold] };
//
//    for(size_t i = 0, j = 0; i < m_capacity; i++){
//        size_t slot = m_permutation->get_value_at(i);
//        cout << "[" << i << "] slot: " << slot << endl;
//        if(slot < m_threshold){
//            cout << "\tsaved" << endl;
//            reordering[j++] = i;
//        }
//    }
//    for(size_t i = 0; i < m_threshold; i++){
//        size_t j = reordering[i];
//        m_permutation->set_value_at(j, i);
//    }
//
//}
//
//
//NoiseDistribution::NoiseDistribution(const NoiseDistribution& copy, size_t begin, size_t end) :
//        m_sequential(copy.m_sequential), m_permutation_ptr(copy.m_permutation_ptr), m_permutation(m_permutation_ptr.get()),
//        m_threshold(copy.m_threshold), m_capacity(copy.m_capacity), m_begin(begin), m_end(end), m_dense(false) {
//
//}
//
//NoiseDistribution::~NoiseDistribution(){
//    // !!! DO NOT DELETE[] this->permutation, already wrapped in a shared_ptr !!!
//    m_permutation = nullptr;
//}
//
//size_t NoiseDistribution::size() const  {
//    return m_end - m_begin;
//}
//
//int64_t NoiseDistribution::key(size_t index) const {
//    index += m_begin;
//
//    auto offset = m_permutation->get_value_at(index);
//    if(offset < m_threshold)
//        return m_sequential->key(offset);
//    else
//        return offset +1;  // +1 to have keys in [1, N] rather than [0, N)
//}
//
//unique_ptr<Distribution> NoiseDistribution::view(size_t start, size_t length) {
//    if(start + length > m_capacity) throw std::invalid_argument("End interval out of bound");
//    return unique_ptr<Distribution>{new NoiseDistribution(*this, start, start + length)};
//}
//
//bool NoiseDistribution::is_dense() const {
//    return m_dense;
//}

} // namespace distribution
