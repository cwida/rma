/*
 * Wrapper for the PMA implementation used in the paper:
 * M. Durand, B. Raffin, F. Faure, A Packed Memory Array to Keep Moving Particles, VRIPHYS 2012
 *
 * The source code of the actual implementation has been provided by the authors
 * of the paper under GPL v3 terms.
 */

#include "dfr.hpp"

#include <cassert>
#include <iostream> // debug
#include <utility>
#include "errorhandling.hpp"

#if defined(HAVE_PMA_DFR)
#include "third-party/pma_dfr_inria/pma.h"
#include "third-party/pma_dfr_inria/pma_aux.h"
#include "third-party/pma_dfr_inria/pma_utils.h"
using namespace dfr;
using namespace dfr::pma;

using namespace std;

#define RAISE(message) RAISE_EXCEPTION(Exception, message)

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[DFR::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/
// Based on the source code from third_party/pma_dfr_inria/pma_iter.h
namespace {
class DFR_Iterator : public ::pma::Iterator {
    ::dfr::pma_struct* m_handle; // pointer to the data structure
    size_t m_current; // current position
    const size_t m_end; // final position or index for this iterator (exclusive)

    void move_next();

public:
    DFR_Iterator(void* handle, size_t begin /* inclusive */, size_t end /* exclusive */);

    bool hasNext() const override;
    std::pair<int64_t, int64_t> next() override;
};

DFR_Iterator::DFR_Iterator(void* handle, size_t begin, size_t end) :
    m_handle(reinterpret_cast<::dfr::pma_struct*>(handle)), m_current(begin), m_end(end){
    if(handle == nullptr) RAISE("Invalid argument, the `handle' is a null pointer");

    move_next(); // fetch the first valid entry
}

void DFR_Iterator::move_next(){
    while(m_current < m_end && !BIT_ISSET(m_handle, m_current)) m_current++;
}

bool DFR_Iterator::hasNext() const {
    return m_current < m_end;
}

std::pair<int64_t, int64_t> DFR_Iterator::next() {
    assert(hasNext() && "Reached the end of the iterator");
    assert(BIT_ISSET(m_handle, m_current) && "No element stored at the current position");
    auto p = reinterpret_cast<pair<int64_t, int64_t>*>ARRAY_ELT(m_handle, m_current);
    m_current++; move_next();
    return *p;
}

} // anonymous ns

/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/
namespace pma { namespace dfr {

// Default parameters taken from the source bench_insertions_k.cpp of the source code
// provided by B. Raffin, 10/05/2018
PackedMemoryArray::PackedMemoryArray() : PackedMemoryArray(0.92, 0.7, 0.08, 0.3, 8) { };

PackedMemoryArray::PackedMemoryArray(double tau_0, double tau_h, double rho_0, double rho_h, unsigned int segment_size) : m_handle(nullptr) {
    m_handle = build_pma(/* initial capacity = */ 8, /* size of each value = */ 8, tau_0, tau_h, rho_0, rho_h, segment_size);
    if(m_handle == nullptr) RAISE("Init error. The function build_pma() returned a null handle");
}

PackedMemoryArray::~PackedMemoryArray(){
    destroy_pma(m_handle); m_handle = nullptr;
}

void PackedMemoryArray::insert(int64_t key, int64_t value){
    COUT_DEBUG("key: " << key << ", value: " << value);
    assert(m_handle != nullptr && "Invalid handle");

    bender::add_elt(m_handle, key, &value);
}

int64_t PackedMemoryArray::find(int64_t key) const {
    COUT_DEBUG("key: " << key);
    auto p = reinterpret_cast<pair<int64_t, int64_t>*>(::dfr::pma::bender::find(m_handle, key));
    if(p == nullptr || p->first != key){
        return -1;
    } else {
        return p->second;
    }
}

size_t PackedMemoryArray::size() const {
    assert(m_handle != nullptr && "Invalid handle");
    // get the cardinality of the top window in the calibrator tree
    auto handle = reinterpret_cast<::dfr::pma_struct*>(m_handle);
    return handle->elts[handle->nb_segments * 2 -2];
}

std::pair<int64_t, int64_t>& PackedMemoryArray::get(uint64_t i) const {
    auto handle = reinterpret_cast<::dfr::pma_struct*>(m_handle);
    assert(i < static_cast<uint64_t>(handle->size) && "Overflow");
    auto ptr = reinterpret_cast<pair<int64_t, int64_t>*>ARRAY_ELT(handle, i);
    return *ptr;
}

std::pair<int64_t, int64_t> PackedMemoryArray::find_interval(int64_t min, int64_t max) const {
    assert(m_handle != nullptr && "Invalid handle");
    auto handle = reinterpret_cast<::dfr::pma_struct*>(m_handle);
    using element_t = pair<int64_t, int64_t>;
    auto array = reinterpret_cast<element_t*>(handle->array);

    auto ebegin = reinterpret_cast<element_t*>(::dfr::pma::bender::find(handle, min));
    int64_t begin = handle->size;
    if(ebegin != nullptr){ begin = ebegin - array; }

    auto eend = reinterpret_cast<element_t*>(::dfr::pma::bender::find(handle, max));
    int64_t end = handle->size;
    if(eend != nullptr) {
        end = eend - array;
        while(eend->first == max && end < static_cast<int64_t>(handle->size)){
            eend++; end++;
        }
    }

    COUT_DEBUG("min: " << min << ", start: " << begin << ", max: " << max << ", end: " << end);

    return std::pair<int64_t, int64_t>{begin, end};
}

unique_ptr<::pma::Iterator> PackedMemoryArray::find(int64_t min, int64_t max) const{
    auto handle = reinterpret_cast<::dfr::pma_struct*>(m_handle);
    auto interval = find_interval(min, max);
    auto begin = interval.first;
    auto end = interval.second;

    return std::unique_ptr<::pma::Iterator>{
      new DFR_Iterator(handle, begin, end)
    };
}

unique_ptr<::pma::Iterator> PackedMemoryArray::iterator() const {
    return unique_ptr<::pma::Iterator>{
        new DFR_Iterator(m_handle, 0, reinterpret_cast<::dfr::pma_struct*>(m_handle)->size)
    };
}

::pma::Interface::SumResult PackedMemoryArray::sum(int64_t min, int64_t max) const {
    auto handle = reinterpret_cast<::dfr::pma_struct*>(m_handle);
    auto interval = find_interval(min, max);
    auto start = interval.first;
    auto end = interval.second;

    SumResult sum;
    sum.m_first_key = sum.m_last_key = std::numeric_limits<int64_t>::min();

    // first element
    while(sum.m_first_key == std::numeric_limits<int64_t>::min() && start < end){
        if(BIT_ISSET(handle, start)){
            sum.m_first_key = get(start).first;
        } else {
            start++;
        }
    }

    // last element
    while(sum.m_last_key == std::numeric_limits<int64_t>::min() && start < end){
        if( BIT_ISSET(handle, (end -1)) ){
            sum.m_last_key = get(end -1).first;
        } else {
            end--;
        }
    }

    // sum the elements in between
    for(auto i = start; i < end; i++){
        if(BIT_ISSET(handle, i)){
           auto& e = get(i);
           sum.m_sum_keys += e.first;
           sum.m_sum_values += e.second;
           sum.m_num_elements++;
        }
    }

    return sum;
}

static int load_sorted_comparator(const void* e1, const void* e2){
    auto p1 = reinterpret_cast<const std::pair<int64_t, int64_t>*>(e1);
    auto p2 = reinterpret_cast<const std::pair<int64_t, int64_t>*>(e2);
    return p1->first > p2->first; // rather than p1 < p2 !
};

void PackedMemoryArray::load_sorted(std::pair<int64_t, int64_t>* array, size_t array_sz) {
#if defined(DEBUG)
    COUT_DEBUG("Loading a batch of `" << array_sz << "' elements");
    for(size_t i = 0; i < array_sz; i++){
        cout << "[" << i << "] <" << array[i].first << ", " << array[i].second << ">\n";
    }
    std::flush(cout);
#endif

    ::dfr::pma::batch::add_array_elts(m_handle, array, array + array_sz, load_sorted_comparator);
}

void PackedMemoryArray::dump() const {
    ::dfr::pma::pma_info_print(m_handle);
    ::dfr::print_pma_levels(m_handle);
    ::dfr::print_data(m_handle,  0, 0);
}

}} // pma::dfr

#else // !defined(HAVE_PMA_DFR)

#define ERROR_NOT_ENABLED RAISE_EXCEPTION(Exception, "Not enabled, source code missing");

namespace pma::dfr {

PackedMemoryArray::PackedMemoryArray() : m_handle(nullptr){
    (void) m_handle; // avoid the warning field not used (-Wunused-private-field)
    ERROR_NOT_ENABLED
}

PackedMemoryArray::PackedMemoryArray(double, double, double, double, unsigned int) : m_handle(nullptr){
    ERROR_NOT_ENABLED
}

PackedMemoryArray::~PackedMemoryArray() { }

std::pair<int64_t, int64_t> PackedMemoryArray::find_interval(int64_t min, int64_t max) const {
    ERROR_NOT_ENABLED
}

std::pair<int64_t, int64_t>& PackedMemoryArray::get(uint64_t i) const{
    ERROR_NOT_ENABLED
}

void PackedMemoryArray::load_sorted(std::pair<int64_t, int64_t>* array, size_t array_sz) {
    ERROR_NOT_ENABLED
}

void PackedMemoryArray::insert(int64_t key, int64_t value){
    ERROR_NOT_ENABLED
}

int64_t PackedMemoryArray::find(int64_t key) const{
    ERROR_NOT_ENABLED
}

std::unique_ptr<pma::Iterator> PackedMemoryArray::find(int64_t min, int64_t max) const {
    ERROR_NOT_ENABLED
}

std::size_t PackedMemoryArray::size() const{
    ERROR_NOT_ENABLED
}

::pma::Interface::SumResult PackedMemoryArray::sum(int64_t min, int64_t max) const {
    ERROR_NOT_ENABLED
}

std::unique_ptr<Iterator> PackedMemoryArray::iterator() const {
    ERROR_NOT_ENABLED
}

void PackedMemoryArray::dump() const {
    ERROR_NOT_ENABLED
}


} // pma::dfr

#endif // defined(HAVE_PMA_DFR) ?
