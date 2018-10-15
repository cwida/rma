/*
 * static_abtree.cpp
 *
 *  Created on: Jan 16, 2018
 *      Author: dleo@cwi.nl
 */

#include "static_abtree.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>

using namespace pma;
using namespace std;
using namespace abtree::static_abtree_detail;

namespace abtree {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[StaticABTree::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Constructors                                                            *
 *                                                                           *
 *****************************************************************************/

StaticABTree::StaticABTree() : StaticABTree(64, 64) { }

StaticABTree::StaticABTree(size_t inode_size, size_t leaf_size) : m_index(inode_size), m_leaf_size(leaf_size) {
    if(inode_size < 3 || leaf_size < 3){ throw std::invalid_argument("[StaticABTree ctor] minimum block size is 4"); }

    m_cardinality = 0;
    m_keys = nullptr;
    m_values = nullptr;
}

StaticABTree::~StaticABTree(){
    delete[] m_keys; m_keys = nullptr;
    delete[] m_values; m_values = nullptr;
}


/******************************************************************************
 *                                                                            *
 *   Miscellaneous                                                            *
 *                                                                            *
 *****************************************************************************/

void StaticABTree::insert(int64_t k, int64_t v){
    m_delta.emplace_back(k, v);
}

size_t StaticABTree::size() const {
    return m_cardinality;
}

bool StaticABTree::empty() const {
    return size() == 0;
}

/******************************************************************************
 *                                                                            *
 *   Build                                                                    *
 *                                                                            *
 *****************************************************************************/

void StaticABTree::build() {
    if(m_delta.empty()) return;

    // sort the delta
    std::sort(begin(m_delta), end(m_delta), [](const auto& e1, const auto& e2){
       return e1.first < e2.first;
    });

    build0();
}

void StaticABTree::build0() {
    int64_t* __restrict keys0 = m_keys;
    int64_t* __restrict values0 = m_values;
    int64_t card0 = m_cardinality;
    size_t card1 = card0 + m_delta.size();
    size_t num_indexed_keys = card1 / m_leaf_size + ((card1 % m_leaf_size) > 0);
    m_index.rebuild(num_indexed_keys);
    int64_t* __restrict keys1 = new int64_t[card1];
    int64_t* __restrict values1 = new int64_t[card1];

    // 1. Create the new arrays of keys/values by merging the content of the old arrays and the sorted deltas
    { // restrict the scope
        size_t i = 0, j = 0, k = 0;
        while(i < card0 && j < m_delta.size()){
            if(keys0[i] < m_delta[j].first){
                keys1[k] = keys0[i];
                values1[k] = values0[i];
                i++;
            } else {
                keys1[k] = m_delta[j].first;
                values1[k] = m_delta[j].second;
                j++;
            }
            k++;
        }
        while(i < card0){
            keys1[k] = keys0[i];
            values1[k] = values0[i];
            i++; k++;
        }
        while(j < m_delta.size()){
            keys1[k] = m_delta[j].first;
            values1[k] = m_delta[j].second;
            j++; k++;
        }
        assert(i == card0 && "It didn't copy all elements from the old array");
        assert(j == m_delta.size() && "It didn't copy all elements from the delta");
        assert(k == card1 && "It didn't fill the new array");
    }

    // 2. Set the separator keys in the index
    for(size_t i = 0; i < num_indexed_keys; i++){
        size_t pos = i * m_leaf_size;
        m_index.set_separator_key(i, keys1[pos]);
    }

    m_delta.clear(); // remove all elements from the delta
    delete[] m_keys;
    m_keys = keys1;
    delete[] m_values;
    m_values = values1;
    m_cardinality = card1;
}

/******************************************************************************
 *                                                                            *
 *   Find                                                                     *
 *                                                                            *
 *****************************************************************************/
int64_t StaticABTree::find(int64_t key) const {
    if(empty()) return -1;

    size_t pos = m_index.find(key) * m_leaf_size;
    pos = scan_array_forwards0(pos, key);

    if( pos < m_cardinality && m_keys[pos] == key){
        return m_values[pos];
    } else {
        return -1;
    }
}

size_t StaticABTree::scan_array_forwards0(size_t offset, int64_t search_key) const {
    while(offset < m_cardinality && m_keys[offset] < search_key) offset++;
    return offset;
}

size_t StaticABTree::scan_array_backwards0(size_t offset, int64_t search_key) const {
    while(offset > 0 && m_keys[offset] > search_key) offset--;
    return offset;
}


pair<size_t, size_t> StaticABTree::find_minmax_indices(int64_t min, int64_t max) const{
    assert(min <= max && !empty());

    size_t index_min = m_index.find_first(min) * m_leaf_size;
    size_t index_max = m_index.find_last(max) * m_leaf_size;

    COUT_DEBUG("index_min: " << index_min << ", index_max: " << index_max);

    // scan left to right for the min
    index_min = scan_array_forwards0(index_min, min);

    // search the max, we may need to proceed either backwards or forwards
    max++; // make an exclusive interval [min, max)
    if(max > m_keys[index_max]) {
        index_max = scan_array_forwards0(index_max, max);
    } else { // max <= m_keys[index_max]
        index_max = scan_array_backwards0(index_max, max);
        if(m_keys[index_max] <= (max -1)) index_max++;
    }

    COUT_DEBUG("[" << min << ", " << max - 1 << "] indx_min: " << index_min << ", indx_max: " << index_max);

    pair<size_t, size_t> minmax;
    minmax.first = index_min;
    minmax.second = index_max;
    return minmax;
}

unique_ptr<pma::Iterator> StaticABTree::find(int64_t min, int64_t max) const {
    if(min > max || empty()) return make_unique<SAB_Iterator>(this, 0, 0); // empty iterator
    auto index_minmax = find_minmax_indices(min, max);
    return make_unique<SAB_Iterator>(this, index_minmax.first, index_minmax.second);
}

/******************************************************************************
 *                                                                            *
 *   Iterator                                                                 *
 *                                                                            *
 *****************************************************************************/
namespace static_abtree_detail {

SAB_Iterator::SAB_Iterator(const StaticABTree* instance, size_t begin, size_t end) : m_instance(instance), m_offset(begin), m_end(end) { }

bool SAB_Iterator::hasNext() const { return m_offset < m_end; }

pair<int64_t, int64_t> SAB_Iterator::next() {
    pair<int64_t, int64_t> result;
    result.first = m_instance->m_keys[m_offset];
    result.second = m_instance->m_values[m_offset];
    m_offset++;
    return result;
}

} // namespace static_abtree_detail

unique_ptr<pma::Iterator> StaticABTree::iterator() const {
    return make_unique<SAB_Iterator>(this, 0, m_cardinality);
}

/******************************************************************************
 *                                                                            *
 *   Range queries / sum                                                      *
 *                                                                            *
 *****************************************************************************/
pma::Interface::SumResult StaticABTree::sum(int64_t min, int64_t max) const {
    SumResult result;
    if(min > max || empty()) return result;

    auto iminmax = find_minmax_indices(min, max);
    // iminmax.first is the begin index of the interval (inclusive)
    // iminmax.second is the end index of the interval (exclusive)
    if(iminmax.second <= iminmax.first) return result;

    int64_t* __restrict keys = m_keys;
    int64_t* __restrict values = m_values;
    result.m_first_key = keys[iminmax.first];
    result.m_num_elements = iminmax.second - iminmax.first;
    for(auto i = iminmax.first; i < iminmax.second; i++){
        result.m_sum_keys += keys[i];
        result.m_sum_values += values[i];
    }
    result.m_last_key = keys[iminmax.second -1];

    return result;
}

/******************************************************************************
 *                                                                            *
 *   Memory footprint                                                         *
 *                                                                            *
 *****************************************************************************/
size_t StaticABTree::memory_footprint() const {
    size_t space_index = m_index.memory_footprint();
    size_t space_arrays = m_cardinality * (sizeof(m_keys[0]) + sizeof(m_values));
    return space_index + space_arrays;
}
/******************************************************************************
 *                                                                            *
 *   Dump                                                                     *
 *                                                                            *
 *****************************************************************************/
void StaticABTree::dump0_array() const{
    assert(!empty());
    cout << "Dense array:\n";
    for(size_t i = 0; i < m_cardinality; i++){
        cout << "[" << i << "] key: " << m_keys[i] << ", value: " << m_values[i] << "\n";
    }
    cout << "\n";
}

void StaticABTree::dump0_delta() const{
    if(m_delta.empty()){
        cout << "-- Delta empty --" << endl;
    } else {
        cout << "Delta:\n";
        for(size_t i = 0; i < m_delta.size();i++){
            auto& e = m_delta[i];
            cout << "[" << i << "] key: " << e.first << ", value: " << e.second << "\n";
        }
    }
}

void StaticABTree::dump() const{
    cout << "[dump] internal node block size: " << m_index.node_size() << ", leaf block size: " << m_leaf_size << ", cardinality: " << m_cardinality << endl;
    if(!empty()){
        m_index.dump(std::cout, nullptr);
        dump0_array();
    }

    dump0_delta();
}

} // namespace abtree
