/*
 * khayyat.hpp
 *
 * Adapted from the source code of Fast and Scalable Inequality Joins, VLDB 2016:
 * http://da.qcri.org/jquiane/jorgequianeFiles/papers/vldbj16.pdf
 *
 * The PMA implementation is itself based on the source code of G. Menghani:
 * https://github.com/reddragon/packed-memory-array/blob/master/impl2.cpp
 *
 */

#include "khayyat.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <iostream>

using namespace std;

namespace pma { namespace khayyat {

static uint64_t log2(uint64_t);
static bool comparator(const element_t& e1, const element_t& e2);

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[Khayyat::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *   Interface                                                               *
 *                                                                           *
 *****************************************************************************/
void PackedMemoryArray::insert(int64_t key, int64_t value){
    insert0(element_t{key, value});

//    dump();
}

int64_t PackedMemoryArray::find(int64_t key) const {
    auto start = lower_bound(key);
    COUT_DEBUG("key: " << key << ", start: " << start);
    if (start == impl.size()) return -1;

    assert(start > -1);
    assert(start < impl.size());

    // Check in a window of size 'w'
    for(int64_t i = start, end = impl.size(); i < end; i++){
        if (present[i]) {
            if (impl[i].first == key) {
                return impl[i].second;
            } else if(impl[i].first > key){
                return -1;
            }
        }
    }

    return -1;
}

std::size_t PackedMemoryArray::size() const {
    return size0();
}

unique_ptr<pma::Iterator> PackedMemoryArray::find(int64_t min, int64_t max) const {
    int64_t start = lower_bound(min);
    int64_t size = impl.size();
    while(start < size && (!present[start] || impl[start].first < min)) start++;
    int64_t end = lower_bound(max);
    while(end < size && (!present[end] || impl[end].first <= max)) end++;

    COUT_DEBUG("min: " << min << ", max: " << max << ", start: " << start << ", end: " << end);

    return unique_ptr<pma::Iterator>{ new JavaIterator{
        BaseIterator(this, start),
        BaseIterator(this, end)
        }
    };
}

unique_ptr<pma::Iterator> PackedMemoryArray::iterator() const {
    return unique_ptr<pma::Iterator>{ new JavaIterator{ begin(), end() } };
}

void PackedMemoryArray::dump() const {
    print();
}

JavaIterator::JavaIterator(const BaseIterator& begin, const BaseIterator& end) : m_current(begin), m_end(end){ }
bool JavaIterator::hasNext() const { return m_current != m_end; }
std::pair<int64_t, int64_t> JavaIterator::next() {
    auto p =  *(m_current++);
    return {p.first, p.second};
};

pma::Interface::SumResult PackedMemoryArray::sum(int64_t min, int64_t max) const {
    int64_t start = lower_bound(min);
    int64_t size = impl.size();
    while(start < size && (!present[start] || impl[start].first < min)) start++;
    int64_t end = lower_bound(max);
    while(end < size && (!present[end] || impl[end].first <= max)) end++;
    COUT_DEBUG("min: " << min << ", max: " << max << ", start: " << start << ", end: " << end);

    SumResult sum;
    sum.m_first_key = impl[start].first;
    for(auto i = start; i < end; i++){
        if(present[i]){
            sum.m_sum_keys += impl[i].first;
            sum.m_sum_values += impl[i].second;
            sum.m_num_elements++;
        }
    }

    // find the last element
    sum.m_last_key = numeric_limits<int64_t>::min();
    while(sum.m_last_key == numeric_limits<int64_t>::min() && end > start){
        if(present[end -1]){
            sum.m_last_key = impl[end -1].first;
        } else {
            end--;
        }
    }

    return sum;

}

/*****************************************************************************
 *                                                                           *
 *   Implementation                                                          *
 *                                                                           *
 *****************************************************************************/

PackedMemoryArray::PackedMemoryArray(size_t capacity) : nelems(0) {
    assert(capacity > 1);
    assert(1 << log2(capacity) == capacity);

    this->init_vars(capacity);
    this->impl.resize(capacity);
    this->present.resize(capacity);
}

void PackedMemoryArray::init_vars(size_t capacity) {
    this->chunk_size = 1 << log2(log2(capacity) * 2);
    assert(this->chunk_size == (1 << log2(this->chunk_size)));
    this->nchunks = capacity / this->chunk_size;
    this->nlevels = log2(this->nchunks);
    this->lgn = log2(capacity);
}

double PackedMemoryArray::upper_threshold_at(int level) const {
    assert(level <= this->nlevels);
    double threshold = 1.0 - ((1.0 - 0.5) * level) / (double) this->lgn;
    return threshold;
}

int64_t PackedMemoryArray::left_interval_boundary(int64_t i, int64_t interval_size) const {
    assert(interval_size == (1 << log2(interval_size)));
    assert(i < (int64_t) this->impl.size());

    int64_t q = i / interval_size;
    int64_t boundary = q * interval_size;
    return boundary;
}

void PackedMemoryArray::resize(size_t capacity) {
    assert(capacity > this->impl.size());
    assert(1 << log2(capacity) == capacity);

    vector<element_t> tmpi(capacity);
    vector<bool> tmpp(capacity);
    double d = (double) capacity / this->nelems;
    size_t ctr = 0;
    for (size_t i = 0; i < this->impl.size(); ++i) {
        if (this->present[i]) {
            size_t idx = d * (ctr++);
            tmpp[idx] = true;
            tmpi[idx] = this->impl[i];
        }
    }
    this->impl.swap(tmpi);
    this->present.swap(tmpp);
    this->init_vars(capacity);
}

void PackedMemoryArray::get_interval_stats(int64_t left, int level, bool &in_limit, int64_t &sz) {
    double t = upper_threshold_at(level);
    int64_t w = static_cast<int64_t>(1 << level) * this->chunk_size;
    sz = 0;
    for (int64_t i = left; i < left + w; ++i) {
        sz += this->present[i] ? 1 : 0;
    }
    double q = (double) (sz + 1) / double(w);
    in_limit = q < t;
}

int64_t PackedMemoryArray::lb_in_chunk(int64_t l, int64_t key) const {
    int64_t i;
    for (i = l; i < l + chunk_size; ++i) {
        if (this->present[i]) {
            if (this->impl[i].first >= key) {
                return i;
            }
        }
    }
    return i;
}

int64_t PackedMemoryArray::lower_bound(int64_t key) const {
    int64_t i;
    if (this->nelems == 0) {
        i = this->impl.size();
    } else {
        int64_t l = 0, r = this->nchunks;
        int64_t m;
        while (l != r) {
            m = l + (r - l) / 2;
            int64_t left = left_interval_boundary(m * chunk_size, chunk_size);
            int64_t pos = lb_in_chunk(left, key);

            COUT_DEBUG("key: " << key << ", left: " << left << ", pos: " << pos);

            // Why does this work? We assume that every chunk of
            // size this->chunk_size contains at least 1
            // element. Hence, if we reach the end of an interval
            // without finding a lower bound, we conclude that all
            // the elements in this chunk are < 'v'. Because every
            // chunk contains at least 1 element, we will never
            // reach the end of an interval because the interval
            // is empty.
            //
            // Note: This is why we need lower density thresholds!
            //
            if (pos == left + chunk_size) {
                // Move to right half
                l = m + 1;
            } else {
                r = m;
            }
        }
        i = l * chunk_size;
    }
    return i;
}

void PackedMemoryArray::insert_merge(int64_t l, element_t v) {
    COUT_DEBUG("segment: " << l << ", element: <" << v.first << ", " << v.second << ">");
    // Insert by merging elements in a window of size 'chunk_size'
    tmp.clear();
    tmp.reserve(this->chunk_size);
    for (int64_t i = l; i < l + this->chunk_size; ++i) {
        if (this->present[i]) {
            this->present[i] = false;
            tmp.push_back(this->impl[i]);
        }
    }

    vector<element_t>::iterator iter = std::lower_bound(tmp.begin(), tmp.end(), v, comparator);
    tmp.insert(iter, v);

    for (int64_t i = 0; i < tmp.size(); ++i) {
        this->present[l + i] = true;
        this->impl[l + i] = tmp[i];
    }
    ++this->nelems;
//    nmoves += chunk_size;
}

void PackedMemoryArray::rebalance_interval(int64_t left, int level) {
    int64_t w = static_cast<int64_t>(1 << level) * this->chunk_size;
    tmp.clear();
    tmp.reserve(w);
    for (int64_t i = left; i < left + w; ++i) {
        if (this->present[i]) {
            tmp.push_back(this->impl[i]);
            this->present[i] = false;
        }
    }
    double m = (double) static_cast<int64_t>(1 << level) * chunk_size / (double) tmp.size();
    assert(m >= 1.0);
    for (int64_t i = 0; i < tmp.size(); ++i) {
        int64_t k = i * m + left;
        assert(k < left + w);
        this->present[k] = true;
        this->impl[k] = tmp[i];
    }
}

void PackedMemoryArray::insert0(element_t v) {
    COUT_DEBUG("element: " << v.first << ", " << v.second);

    int64_t i = lower_bound(v.first);
    if (i == this->impl.size()) {
        --i;
    }
    assert(i > -1);
    assert(i < this->impl.size());

    // Check in a window of size 'w'
    int64_t w = chunk_size;
    int level = 0;
    int64_t l = this->left_interval_boundary(i, w);
    COUT_DEBUG("position: " << i << ", segment: " << l << ", num_segments: " << nchunks << ", segment size: " << chunk_size);

    // Number of elements in current window. We just need sz to be
    // less than w -- we don't need the exact value of 'sz' here.
    int64_t sz = w - 1;

    bool in_limit = false;

    // If the current chunk has space, then the last element will
    // be unused (with significant probability). First check that
    // as a quick check.
    if (this->present[l + this->chunk_size - 1]) {
        get_interval_stats(l, level, in_limit, sz);
    }

    if (sz < w) {
        // There is some space in this interval. We can just
        // shuffle elements and insert.
        this->insert_merge(l, v);
    } else {
        // No space in this interval. Find an interval above this
        // interval that is within limits, re-balance, and
        // re-start insertion.
        in_limit = false;
        while (!in_limit) {
            w *= 2;
            level += 1;
            // assert(level <= this->nlevels);
            if (level > this->nlevels) {
                // Root node is out of balance. Resize array.
                this->resize(2 * this->impl.size());
                this->insert0(v);
                return;
            }

            l = this->left_interval_boundary(i, w);
            get_interval_stats(l, level, in_limit, sz);
        }
        this->rebalance_interval(l, level);
        this->insert0(v);
    }

}

size_t PackedMemoryArray::size0() const {
    return this->nelems;
}

BaseIterator PackedMemoryArray::begin() const {
    return BaseIterator(this, 0);
}

BaseIterator PackedMemoryArray::end() const {
    return BaseIterator(this, this->impl.size());
}

void PackedMemoryArray::print() const {
    for (size_t i = 0; i < this->impl.size(); ++i) {
        cout << "[" << i << "] ";
        if(present[i]){
            cout << "key: " << impl[i].first << ", value: " << impl[i].second;
        }
        cout << "\n";
    }
}


/*****************************************************************************
 *                                                                           *
 *   BaseIterator                                                            *
 *                                                                           *
 *****************************************************************************/

BaseIterator::BaseIterator(const PackedMemoryArray *p, int64_t _i) : pma(p), i(_i) { }
BaseIterator::BaseIterator(const BaseIterator &rhs) {
    this->pma = rhs.pma;
    this->i = rhs.i;
}

BaseIterator& BaseIterator::operator=(BaseIterator &rhs) {
    this->pma = rhs.pma;
    this->i = rhs.i;
    return *this;
}

BaseIterator& BaseIterator::operator++() {
    if (i < (int64_t) pma->impl.size()) ++i;
    while (i < (int64_t) pma->impl.size() && !pma->present[i]) { ++i; }
    return *this;
}

BaseIterator BaseIterator::operator++(int) {
    BaseIterator tmp = *this;
    ++(*this);
    return tmp;
}

bool BaseIterator::operator==(BaseIterator rhs) const {
    return this->pma == rhs.pma && this->i == rhs.i;
}

bool BaseIterator::operator!=(BaseIterator rhs) const {
    return !(*this == rhs);
}

const element_t& BaseIterator::operator*() {
    assert(pma->present[this->i]);
    return pma->impl[this->i];
}

element_t BaseIterator::get() {
    assert(pma->present[this->i]);
    return pma->impl[this->i];
}

const element_t* BaseIterator::operator->() {
    assert(pma->present[this->i]);
    return &(pma->impl[this->i]);
}

const element_t* BaseIterator::next() {
    assert(pma->present[this->i]);
    return &(pma->impl[this->i]);
}

/*****************************************************************************
 *                                                                           *
 *   Helpers                                                                 *
 *                                                                           *
 *****************************************************************************/
static uint64_t log2(uint64_t n) {
    uint64_t lg2 = 0;
    while (n > 1) {
        n /= 2;
        ++lg2;
    }
    return lg2;
}

static bool comparator(const element_t& e1, const element_t& e2){
    return e1.first < e2.first;
}


}} // pma::khayyat


