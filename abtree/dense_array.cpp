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

#include "dense_array.hpp"

#include <algorithm> // std::sort
#include <cassert>
#include <cerrno>
#include <cstring> // strerror
#include <linux/memfd.h> // MFD_HUGETLB
#include <iostream>
#include <sys/mman.h> // mmap
#include <unistd.h> // close

#include "configuration.hpp"
#include "errorhandling.hpp"
#include "miscellaneous.hpp"

using namespace pma;
using namespace std;

DEFINE_EXCEPTION(DenseArrayRuntimeException);
#define RAISE(msg) RAISE_EXCEPTION(DenseArrayRuntimeException, msg)

namespace abtree {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[DenseArray::" << __FUNCTION__ << "] " << msg << std::endl
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

DenseArray::DenseArray(size_t node_size) : m_index(node_size), m_keys(nullptr), m_values(nullptr), m_cardinality(0) { }

DenseArray::~DenseArray() {
    release_memory(m_handle_physical_memory_keys, m_handle_physical_memory_values, m_keys, m_values, m_cardinality);
}


/******************************************************************************
 *                                                                            *
 *   Miscellaneous                                                            *
 *                                                                            *
 *****************************************************************************/

void DenseArray::insert(int64_t k, int64_t v){
    m_delta.emplace_back(k, v);
}

size_t DenseArray::size() const {
    return m_cardinality;
}

bool DenseArray::empty() const {
    return m_cardinality == 0;
}

/******************************************************************************
 *                                                                            *
 *   Build                                                                    *
 *                                                                            *
 *****************************************************************************/
void DenseArray::build() {
    if(m_delta.empty()) return; // nop

    // sort the delta
    std::sort(begin(m_delta), end(m_delta), [](const auto& e1, const auto& e2){
       return e1.first < e2.first;
    });

    // acquire some physical memory
    uint64_t cardinality_new = m_cardinality + m_delta.size();
    int handle_keys_new (-1), handle_values_new (-1);
    int64_t *keys_new(nullptr), *values_new(nullptr);
    acquire_memory(&handle_keys_new, &handle_values_new, &keys_new, &values_new, cardinality_new);
    auto dealloc = [&, this](int64_t*){ release_memory(handle_keys_new, handle_values_new, keys_new, values_new, cardinality_new); };
    unique_ptr<int64_t, decltype(dealloc)> protect_from_memory_leak{ (int64_t*) 0x1, dealloc };

    // merge
    int64_t *__restrict keys_old(m_keys), *__restrict values_old(m_values);
    uint64_t cardinality_old = m_cardinality;
    uint64_t cardinality_delta = m_delta.size();
    uint64_t i(0), j(0), k(0);
    while(i < cardinality_old && j < cardinality_delta){
        assert(k < cardinality_new && "Invalid index, over the actual capacity of the new arrays");
        if(keys_old[i] < m_delta[j].first){
            keys_new[k] = keys_old[i];
            values_new[k] = values_old[i];
            i++;
        } else {
            keys_new[k] = m_delta[j].first;
            values_new[k] = m_delta[j].second;
            j++;
        }
        k++;
    }
    while(i < cardinality_old){
        keys_new[k] = keys_old[i];
        values_new[k] = values_old[i];
        i++; k++;
    }
    while(j < cardinality_delta){
        keys_new[k] = m_delta[j].first;
        values_new[k] = m_delta[j].second;
        j++; k++;
    }

    // rebuild the index
    const uint64_t node_size = m_index.node_size();
    const uint64_t cardinality_index = cardinality_new / node_size + ((cardinality_new % node_size) > 0);
    m_index.rebuild(cardinality_index);
    for(uint64_t i = 0; i < cardinality_index; i++){ // `i' shadows the previous var for the merge
        m_index.set_separator_key(i, keys_new[i * node_size]);
    }

    m_delta.clear(); // remove all elements from the delta
    release_memory(m_handle_physical_memory_keys, m_handle_physical_memory_values, m_keys, m_values, m_cardinality);
    m_keys = keys_new;
    m_values = values_new;
    m_cardinality = cardinality_new;
    m_handle_physical_memory_keys = handle_keys_new;
    m_handle_physical_memory_values = handle_values_new;

    protect_from_memory_leak.release();
}

/******************************************************************************
 *                                                                            *
 *   Memory handling                                                          *
 *                                                                            *
 *****************************************************************************/
static int g_internal_id = 0;

void DenseArray::acquire_memory(int* out_handle_keys, int* out_handle_values, int64_t** out_array_keys, int64_t** out_array_values, uint64_t cardinality){
    // check the parameters are not null
    if(out_handle_values == nullptr || out_handle_values == nullptr || out_array_keys == nullptr || out_array_values == nullptr) RAISE("Null parameter");

    int rc = 0;
    uint64_t memory_required_bytes = get_amount_memory_needed(cardinality);

    // init
    *out_handle_keys = *out_handle_values = -1;
    *out_array_keys = *out_array_values = nullptr;
    auto dealloc = [&](int64_t*){ release_memory(*out_handle_keys, *out_handle_values, *out_array_keys, *out_array_values, cardinality); };
    unique_ptr<int64_t, decltype(dealloc)> protect_from_memory_leak{ (int64_t*) 0x1, dealloc };

    // create the physical handles
    string id_handle_keys = "dense_array_keys_"; id_handle_keys += to_string(g_internal_id);
    string id_handle_values = "dense_array_values_"; id_handle_values += to_string(g_internal_id);
    g_internal_id++;
    int memfd_flags = configuration::use_huge_pages() ? MFD_HUGETLB : 0;
    *out_handle_keys = memfd_create(id_handle_keys.c_str(), memfd_flags);
    if(*out_handle_keys < 0){ RAISE("Cannot allocate the physical memory (keys); memfd_create error: " << strerror(errno) << "(" << errno << ")"); }
    *out_handle_values = memfd_create(id_handle_values.c_str(), memfd_flags);
    if(*out_handle_values < 0){ RAISE("Cannot allocate the physical memory (values); memfd_create error: " << strerror(errno) << "(" << errno << ")"); }

    // allocate the memory
    COUT_DEBUG("Allocating two arrays of " << memory_required_bytes << " bytes to serve " << cardinality << " keys/values");
    rc = ftruncate(*out_handle_keys, memory_required_bytes);
    if(rc != 0){ RAISE("Cannot allocate the physical memory (keys). ftruncate error: " << strerror(errno) << "(" << errno << ")"); }
    rc = ftruncate(*out_handle_values, memory_required_bytes);
    if(rc != 0){ RAISE("Cannot allocate the physical memory (values). ftruncate error: " << strerror(errno) << "(" << errno << ")"); }

    // create the virtual mappings
    void* mmap_keys = mmap(
        /* starting address, NULL means arbitrary */ NULL,
        /* length in bytes */ memory_required_bytes,
        /* memory protection */ PROT_READ | PROT_WRITE,
        /* flags */ MAP_SHARED,
        /* file descriptor */ *out_handle_keys,
        /* offset, in terms of multiples of the page size */ 0);
    if(mmap_keys == MAP_FAILED){ RAISE("Cannot allocate the virtual memory: " << memory_required_bytes << " bytes (keys). mmap error: " << strerror(errno) << "(" << errno << ")"); }
    void* mmap_values = mmap(
        /* starting address, NULL means arbitrary */ NULL,
        /* length in bytes */ memory_required_bytes,
        /* memory protection */ PROT_READ | PROT_WRITE,
        /* flags */ MAP_SHARED,
        /* file descriptor */ *out_handle_values,
        /* offset, in terms of multiples of the page size */ 0);
    if(mmap_values == MAP_FAILED){ RAISE("Cannot allocate the virtual memory: " << memory_required_bytes << " bytes (values). mmap error: " << strerror(errno) << "(" << errno << ")"); }

    *out_array_keys = reinterpret_cast<int64_t*>(mmap_keys);
    *out_array_values = reinterpret_cast<int64_t*>(mmap_values);

    protect_from_memory_leak.release(); // we're done
}

void DenseArray::release_memory(int& handle_keys, int& handle_values, int64_t*& array_keys, int64_t*& array_values, uint64_t cardinality){
    uint64_t amount_mapped_memory = get_amount_memory_needed(cardinality);

    // release the virtual mappings
    if(array_keys != nullptr){
        int rc = munmap(array_keys, amount_mapped_memory);
        if(rc < 0){
            cerr << "[DenseArray::release_memory] Error in releasing the virtual memory for the keys, munmap error: " << strerror(errno) << " (" << errno << ")" << endl;
        }
        array_keys = nullptr;
    }
    if(array_values != nullptr){
        int rc = munmap(array_values, amount_mapped_memory);
        if(rc < 0){
            cerr << "[DenseArray::release_memory] Error in releasing the virtual memory for the values, munmap error: " << strerror(errno) << " (" << errno << ")" << endl;
        }
        array_values = nullptr;
    }


    // release the physical memory
    if(handle_keys >= 0){
        int rc = close(handle_keys);
        if(rc < 0){
            cerr << "[DenseArray::release_memory] Error in releasing the physical memory for the keys, fh: " << handle_keys << ": " << strerror(errno) << " (" << errno << ")" << endl;
        }
        handle_keys = -1;
    }
    if(handle_values >= 0){
        int rc = close(handle_values);
        if(rc < 0){
            cerr << "[DenseArray::release_memory] Error in releasing the physical memory for the values, fh: " << handle_values << ": " << strerror(errno) << " (" << errno << ")" << endl;
        }
        handle_values = -1;
    }
}

uint64_t DenseArray::get_amount_memory_needed(uint64_t cardinality){
    uint64_t page_size = get_memory_page_size();
    uint64_t num_pages = ((cardinality*sizeof(uint64_t)) / page_size) + (((cardinality*sizeof(uint64_t)) % page_size) >0);
    return page_size * num_pages;
}

/******************************************************************************
 *                                                                            *
 *   Find                                                                     *
 *                                                                            *
 *****************************************************************************/
int64_t DenseArray::find(int64_t key) const {
    if(empty()) return -1;

    size_t i = m_index.find_first(key) * m_index.node_size();
    int64_t* __restrict keys = m_keys;
    while(i < m_cardinality && keys[i] < key) i++;
    return (i < m_cardinality && keys[i] == key) ? m_values[i] : -1;
}

unique_ptr<pma::Iterator> DenseArray::find(int64_t min, int64_t max) const {
    if(min > max || empty()) return make_unique<InternalIterator>(this, 0, 0); // empty iterator
    int64_t node_size = m_index.node_size();
    int64_t index_first = m_index.find_first(min) * node_size;
    int64_t end = m_cardinality;
    int64_t* __restrict keys = m_keys;
    while(index_first < end && keys[index_first] < min) index_first++;
    int64_t index_last = m_index.find_last(max) * node_size;
    while(index_last < end && keys[index_last] <= max) index_last++;

    COUT_DEBUG("[" << min << ", " << max << "] index_first: " << index_first << ", index_last: " << index_last);

    return make_unique<InternalIterator>(this, index_first, index_last);
}

unique_ptr<pma::Iterator> DenseArray::iterator() const {
    return make_unique<InternalIterator>(this, 0, m_cardinality);
}

DenseArray::InternalIterator::InternalIterator(const DenseArray* instance, size_t begin, size_t end) :
        m_keys(instance->m_keys), m_values(instance->m_values), m_offset(begin), m_end(end) { }

bool DenseArray::InternalIterator::hasNext() const { return m_offset < m_end; }

pair<int64_t, int64_t> DenseArray::InternalIterator::next() {
    pair<int64_t, int64_t> result;
    result.first = m_keys[m_offset];
    result.second = m_values[m_offset];
    m_offset++;
    return result;
}

/******************************************************************************
 *                                                                            *
 *   Sum                                                                      *
 *                                                                            *
 *****************************************************************************/
pma::Interface::SumResult DenseArray::sum(int64_t min, int64_t max) const {
    SumResult result;
    if(min > max || empty()) return result;

    int64_t* __restrict keys = m_keys;
    int64_t* __restrict values = m_values;

    int64_t node_size = m_index.node_size();
    int64_t offset = m_index.find_first(min) * node_size;
    while(offset < m_cardinality && keys[offset] < min) offset++;
    int64_t end = m_index.find_last(max) * node_size;
    while(end < m_cardinality && keys[end] <= max) end++;

    if(offset >= end) return result;

    result.m_first_key = keys[offset];
    result.m_last_key = keys[end -1];
    result.m_num_elements = end - offset;
    do {
        result.m_sum_keys += keys[offset];
        result.m_sum_values += values[offset];
        offset++;
    } while(offset < end);

    return result;
}

/******************************************************************************
 *                                                                            *
 *   Dump                                                                     *
 *                                                                            *
 *****************************************************************************/
size_t DenseArray::memory_footprint() const {
    return get_amount_memory_needed(m_cardinality) *2 /* x2 = keys and values */ + m_index.memory_footprint();
}

void DenseArray::dump() const {
    m_index.dump();

    cout << "[Dense arrays] cardinality: " << m_cardinality << ", memory footprint: 2x " << get_amount_memory_needed(m_cardinality) << " bytes" << endl;
    if(m_cardinality > 0){
        for(uint64_t i = 0; i < m_cardinality; i++){
            cout << "[" << i << "] key: " << m_keys[i] << ", value: " << m_values[i] << "\n";
        }
        cout << "\n\n";
    }

    cout << "[Delta] cardinality: " << m_delta.size() << endl;
    for(uint64_t i = 0; i < m_delta.size(); i++){
        cout << "[" << i << "] key: " << m_delta[i].first << ", value: " << m_delta[i].second << "\n";
    }
}

} /* namespace abtree */
