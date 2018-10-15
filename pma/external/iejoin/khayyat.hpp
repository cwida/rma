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

#ifndef PMA_EXTERNAL_IEJOIN_KHAYYAT_HPP_
#define PMA_EXTERNAL_IEJOIN_KHAYYAT_HPP_

#include <cinttypes>
#include <utility>
#include <vector>

#include "pma/interface.hpp"
#include "pma/iterator.hpp"

namespace pma { namespace khayyat {

using element_t = std::pair<int64_t, int64_t>;
class BaseIterator;
class JavaIterator;
class PackedMemoryArray; // forward declaration

class PackedMemoryArray : public pma::InterfaceRQ {
    friend class BaseIterator;
    std::vector<element_t> impl;
    size_t nelems = 0;
    std::vector<bool> present;
    int64_t chunk_size = 0;
    int64_t nchunks = 0;
    int nlevels = 0;
    int64_t lgn = 0;
    std::vector<element_t> tmp;

    double upper_threshold_at(int level) const;
    void init_vars(size_t capacity);
    int64_t left_interval_boundary(int64_t i, int64_t interval_size) const;
    void resize(size_t capacity);
    void get_interval_stats(int64_t left, int level, bool &in_limit, int64_t &sz);
    int64_t lb_in_chunk(int64_t l, int64_t key) const;
    int64_t lower_bound(int64_t key) const;
    void insert_merge(int64_t l, element_t v);
    void rebalance_interval(int64_t left, int level);
    void insert0(element_t v);
    size_t size0() const;
    BaseIterator begin() const;
    BaseIterator end() const;
    void print() const;

public:
    PackedMemoryArray(size_t capacity = 2);

    void insert(int64_t key, int64_t value) override;
    int64_t find(int64_t key) const override;
    std::size_t size() const override;
    std::unique_ptr<pma::Iterator> find(int64_t min, int64_t max) const override;
    std::unique_ptr<pma::Iterator> iterator() const override;
    pma::Interface::SumResult sum(int64_t min, int64_t max) const override;
    void dump() const override;
};

class BaseIterator {
    const PackedMemoryArray *pma;
    int64_t i;

public:
    BaseIterator(const PackedMemoryArray *p, int64_t _i);
    BaseIterator(const BaseIterator &rhs);
    BaseIterator& operator=(BaseIterator &rhs);
    BaseIterator& operator++();
    BaseIterator operator++(int);
    bool operator==(BaseIterator rhs) const;
    bool operator!=(BaseIterator rhs) const;
    const element_t& operator*();
    element_t get();
    const element_t* operator->();
    const element_t* next();
};

class JavaIterator : public ::pma::Iterator {
    BaseIterator m_current;
    BaseIterator m_end;

public:
    JavaIterator(const BaseIterator& begin, const BaseIterator& end);
    bool hasNext() const override;
    std::pair<int64_t, int64_t> next() override;
};


}} // pma::khayyat



#endif /* PMA_EXTERNAL_IEJOIN_KHAYYAT_HPP_ */
