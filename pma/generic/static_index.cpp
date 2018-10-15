/*
 * static_index.cpp
 *
 *  Created on: Sep 4, 2018
 *      Author: dleo@cwi.nl
 */

#include "static_index.hpp"
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <iomanip>
#include <iostream>
#include <stdexcept>

using namespace std;

namespace pma {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[StaticIndex::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

StaticIndex::StaticIndex(uint64_t node_size, uint64_t num_segments) :
        m_node_size(node_size), m_height(0), m_capacity(0), m_keys(nullptr), m_key_minimum(numeric_limits<int64_t>::max()) {
    if(node_size > (uint64_t) numeric_limits<uint16_t>::max()){ throw std::invalid_argument("Invalid node size: too big"); }
    rebuild(num_segments);
}

StaticIndex::~StaticIndex(){
    free(m_keys); m_keys = nullptr;
}

int64_t StaticIndex::node_size() const noexcept {
    // cast to int64_t
    return m_node_size;
}

void StaticIndex::rebuild(uint64_t N){
    if(N == 0) throw std::invalid_argument("Invalid number of keys: 0");
    int height = ceil( log2(N) / log2(node_size()) );
    if(height > m_rightmost_sz){ throw std::invalid_argument("Invalid number of keys/segments: too big"); }
    uint64_t tree_sz = pow(node_size(), height) -1; // don't store the minimum, segment 0

    if(height != m_height){
        free(m_keys); m_keys = nullptr;
        int rc = posix_memalign((void**) &m_keys, /* alignment */ 64,  /* size */ tree_sz * sizeof(int64_t));
        if(rc != 0) { throw std::bad_alloc(); }
        m_height = height;
    }
    m_capacity = N;
    COUT_DEBUG("capacity: " << m_capacity << ", height: " << m_height);

    // set the height of all rightmost subtrees
    while(height > 0){
        assert(height > 0);
        uint64_t subtree_sz = pow(node_size(), height -1);
        m_rightmost[height - 1].m_root_sz = (N -1) / subtree_sz;
        assert(m_rightmost[height -1].m_root_sz > 0);
        uint64_t rightmost_subtree_sz = (N -1) % subtree_sz;
        int rightmost_subtree_height = 0;
        if(rightmost_subtree_sz > 0){
            rightmost_subtree_sz += 1; // with B-1 keys we index B entries
            rightmost_subtree_height = ceil(log2(rightmost_subtree_sz) / log2(m_node_size));
        }
        m_rightmost[height -1].m_right_height = rightmost_subtree_height;

        COUT_DEBUG("height: " << height << ", rightmost subtree height: " << rightmost_subtree_height << ", root_sz: " << m_rightmost[height - 1].m_root_sz);

        // next subtree
        N = rightmost_subtree_sz;
        height = rightmost_subtree_height;
    }

    // set the pointer to the first leaf
//    m_ptr_first_leaf = get_slot(1);
}

int StaticIndex::height() const noexcept {
    return m_height;
}


size_t StaticIndex::memory_footprint() const {
    return (pow(node_size(), height()) -1) * sizeof(int64_t);
}

/*****************************************************************************
 *                                                                           *
 *   Separator keys                                                          *
 *                                                                           *
 *****************************************************************************/
int64_t* StaticIndex::get_slot(uint64_t segment_id) const {
    COUT_DEBUG("segment_id: " << segment_id);
    assert(segment_id > 0 && "The segment 0 is not explicitly stored");
    assert(segment_id < static_cast<uint64_t>(m_capacity) && "Invalid slot");

    int64_t* __restrict base = m_keys;
    int64_t offset = segment_id;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        int64_t subtree_id = offset / subtree_sz;
        int64_t modulo = offset % subtree_sz;
        COUT_DEBUG("height: " << height << ", base: " << (base - m_keys) << ", subtree_id: " << subtree_id << ", modulo: " << modulo << ", rightmost: " << rightmost);

        if(modulo == 0){ // found, this is an internal node
            assert(subtree_id > 0 && "Otherwise this would have been an internal element on an ancestor");
            return base + subtree_id -1;
        }

        // traverse the children
        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset -= subtree_id * subtree_sz;

        // is this the rightmost subtree ?
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
            COUT_DEBUG("rightmost, height: " << height << ", subtree_sz: " << subtree_sz);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    return base + offset;
}

void StaticIndex::set_separator_key(uint64_t segment_id, int64_t key){
    if(segment_id == 0) {
        m_key_minimum = key;
    } else {
        get_slot(segment_id)[0] = key;
    }

    assert(get_separator_key(segment_id) == key);
}

int64_t StaticIndex::get_separator_key(uint64_t segment_id) const {
    if(segment_id == 0)
        return m_key_minimum;
    else
        return get_slot(segment_id)[0];
}

/*****************************************************************************
 *                                                                           *
 *   Find                                                                    *
 *                                                                           *
 *****************************************************************************/
uint64_t StaticIndex::find(int64_t key) const noexcept {
    COUT_DEBUG("key: " << key);
    if(key <= m_key_minimum) return 0; // easy!

    int64_t* __restrict base = m_keys;
    int64_t offset = 0;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        uint64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
        uint64_t subtree_id = 0;
        while(subtree_id < root_sz && base[subtree_id] <= key) subtree_id++;

        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset += subtree_id * subtree_sz;

        COUT_DEBUG("height: " << height << ", base: " << (base - m_keys) << ", subtree_id: " << subtree_id << ", offset: " << offset << ", rightmost: " << rightmost);

        // similar to #get_slot
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
            COUT_DEBUG("rightmost, height: " << height << ", subtree_sz: " << subtree_sz);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    COUT_DEBUG("offset: " << offset);
    return offset;
}

uint64_t StaticIndex::find_first(int64_t key) const noexcept {
    if(key < m_key_minimum) return 0; // easy!

    int64_t* __restrict base = m_keys;
    int64_t offset = 0;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        uint64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
        uint64_t subtree_id = 0;
        while(subtree_id < root_sz && base[subtree_id] < key) subtree_id++;

        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset += subtree_id * subtree_sz;

        // similar to #get_slot
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    return offset;
}

uint64_t StaticIndex::find_last(int64_t key) const noexcept {
    if(key < m_key_minimum) return 0; // easy!

    int64_t* __restrict base = m_keys;
    int64_t offset = 0;
    int height = m_height;
    bool rightmost = true; // this is the rightmost subtree
    int64_t subtree_sz = pow(node_size(), height -1);

    while(height > 0){
        uint64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
        uint64_t subtree_id = root_sz;
        while(subtree_id > 0 && key < base[subtree_id -1]) subtree_id--;

        base += (node_size() -1) + subtree_id * (subtree_sz -1);
        offset += subtree_id * subtree_sz;

        // similar to #get_slot
        rightmost = rightmost && (subtree_id >= m_rightmost[height -1].m_root_sz);
        if(rightmost){
            height = m_rightmost[height -1].m_right_height;
            subtree_sz = pow(node_size(), height -1);
        } else {
            height --;
            subtree_sz /= node_size();
        }
    }

    return offset;
}

int64_t StaticIndex::minimum() const noexcept {
    return m_key_minimum;
}

/*****************************************************************************
 *                                                                           *
 *   Dump                                                                    *
 *                                                                           *
 *****************************************************************************/

static void dump_tabs(std::ostream& out, size_t depth){
    using namespace std;

    auto flags = out.flags();
    out << setw((depth-1) * 2 + 5) << setfill(' ') << ' ';
    out.setf(flags);
}

void StaticIndex::dump_subtree(std::ostream& out, int64_t* root, int height, bool rightmost, int64_t fence_min, int64_t fence_max, bool* integrity_check) const {
    if(height <= 0) return; // base case

    int depth = m_height - height +1;
    int64_t root_sz = (rightmost) ? m_rightmost[height -1].m_root_sz : node_size() -1; // full
    int64_t subtree_sz = pow(node_size(), height -1);

    // preamble
    auto flags = out.flags();
    if(depth > 1) out << ' ';
    out << setw((depth -1) * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << "offset: " << root - m_keys << ", root size: " << root_sz << ", fence keys (interval): [" << fence_min << ", " << fence_max << "]\n";
    out.setf(flags);

    dump_tabs(out, depth);
    out << "keys: ";
    for(size_t i = 0; i < root_sz; i++){
        if(i > 0) out << ", ";
        out << (i+1) << " => k:" << (i+1) * subtree_sz << ", v:" << root[i];

        if(i == 0 && root[0] < fence_min){
            out << " (ERROR: smaller than the min fence key: " << fence_min << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(i > 0 && root[i] < root[i-1]){
            out << " (ERROR: sorted order not respected: " << root[i-1] << " > " << root[i] << ")";
            if(integrity_check) *integrity_check = false;
        }
        if(i == root_sz -1 && root[i] > fence_max){
            out << " (ERROR: greater than the max fence key: " << fence_max << ")";
            if(integrity_check) *integrity_check = false;
        }

    }
    out << "\n";

    if(height > 1) { // internal node?
        int64_t* base = root + node_size() -1;

        dump_tabs(out, depth);
        out << "offsets: ";
        for(size_t i = 0; i <= root_sz; i++){
          if(i > 0) out << ", ";
          out << i << ": " << (base + i * (subtree_sz -1)) - m_keys;
        }
        out << "\n";

        // recursively dump the children
        for(size_t i = 0; i < root_sz; i++){
            int64_t fmin = (i == 0) ? fence_min : root[i-1];
            int64_t fmax = root[i];

            dump_subtree(out, base + (i* (subtree_sz -1)), height -1, false, fmin, fmax, integrity_check);
        }

        // dump the rightmost subtree
        dump_subtree(out, base + root_sz * (subtree_sz -1), m_rightmost[height -1].m_right_height, rightmost, root[root_sz -1], fence_max, integrity_check);
    }
}

void StaticIndex::dump(std::ostream& out, bool* integrity_check) const {
    out << "[Index] block size: " << node_size() << ", height: " << height() <<
            ", capacity (number of entries indexed): " << m_capacity << ", minimum: " << minimum() << "\n";

    if(m_capacity > 1)
        dump_subtree(out, m_keys, height(), true, m_key_minimum, numeric_limits<int64_t>::max(), integrity_check);
}

void StaticIndex::dump() const {
    dump(cout);
}

std::ostream& operator<<(std::ostream& out, const StaticIndex& index){
    index.dump(out);
    return out;
}

} // namespace pma


