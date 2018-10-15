/*
 * pma.cpp
 *
 *  Extracted and adapted from
 *  https://github.com/desert0616/gpma_bfs_demo/blob/master/cpu_baseline/containers/pma_dynamic_graph.hpp
 *  Original author: Mo Sha
 *
 *  ---------------------------------------------------------------------------
 *
 *  MIT License
 *
 *  Copyright (c) 2018 Mo Sha
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in al
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE
 *
 */

#include "pma.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>
#include <stdexcept>

namespace pma { namespace sha {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[pma::sha::PMA::" << __FUNCTION__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

/*****************************************************************************
 *                                                                           *
 *  Interface                                                                *
 *                                                                           *
 *****************************************************************************/
PMA::PMA() {
    init_pma();
}

void PMA::insert(int64_t key, int64_t value){
    insert_pma(ELEMENT_TYPE{key, value});
}

int64_t PMA::find(int64_t key) const {
    int64_t start = locate_segment(key);
    int64_t end = start + segment_length_;

    for(int64_t i = start; i < end && element_exist_[i]; i++){
        if(data_[i].first == key){
            return data_[i].second;
        }
    }

    return -1;
}

size_t PMA::size() const {
    return element_cnt_;
}

std::unique_ptr<::pma::Iterator> PMA::find(int64_t min, int64_t max) const {
    auto interval = find_interval(min, max);

    return std::unique_ptr<::pma::Iterator>{
      new pma::sha::Iterator(data_, element_exist_, interval.first, interval.second)
    };
}

pma::Interface::SumResult PMA::sum(int64_t min, int64_t max) const {
    auto interval = find_interval(min, max);
    auto start = interval.first;
    auto end = interval.second;
    if(end <= start) return SumResult{};

    SumResult sum;

    sum.m_first_key = data_[start].first;
    for(auto i = start; i < end; i++){
        if(element_exist_[i]){
            sum.m_sum_keys += data_[i].first;
            sum.m_sum_values += data_[i].second;
            sum.m_num_elements++;
        }
    }

    // find the last key
    sum.m_last_key = std::numeric_limits<int64_t>::min();
    while(sum.m_last_key == std::numeric_limits<int64_t>::min() && end > start){
        if(element_exist_[end -1]){
            sum.m_last_key = data_[end -1].first;
        } else {
            end--;
        }
    }


    return sum;
}

std::unique_ptr<::pma::Iterator> PMA::iterator() const {
    return std::unique_ptr<pma::Iterator>{
      new pma::sha::Iterator(data_, element_exist_, 0, data_.size())
    };
}

void PMA::dump() const {
    using namespace std;
    cout << "[Dump]\n";
    for(size_t i = 0; i < data_.size(); i++){
        cout << "[" << i << "] present: " << element_exist_[i];
        if(element_exist_[i]){
            cout << ", element: <" << data_[i].first << ", " << data_[i].second << ">";
        }
        cout << "\n";
    }

    validate();
}

/*****************************************************************************
 *                                                                           *
 *  Iterator                                                                 *
 *                                                                           *
 *****************************************************************************/
// based on pma_iterator, from https://github.com/desert0616/gpma_bfs_demo/blob/master/cpu_baseline/containers/pma_dynamic_graph.hpp
Iterator::Iterator(const std::vector<ELEMENT_TYPE> &data, const std::vector<bool> &element_exist, int64_t start /* inclusive */, int64_t end /* exclusive */) :
        real_idx_(start), data_(data), element_exist_(element_exist), end(end) {
    move_next();
}

void Iterator::move_next(){
    while(real_idx_ < end && !element_exist_[real_idx_]) real_idx_++;
}

bool Iterator::hasNext() const {
    return real_idx_ < end;
}

std::pair<int64_t, int64_t> Iterator::next() {
    assert(hasNext() && "Reached the end of the iterator");
    assert(element_exist_[real_idx_] && "No element stored at the current position");
    auto result = data_[real_idx_];
    real_idx_++; move_next();
    return result;
}

/*****************************************************************************
 *                                                                           *
 *  Validate                                                                 *
 *                                                                           *
 *****************************************************************************/
void PMA::validate() const{
    std::cout << "[Validate] Cardinality: " << size() << ", capacity: " << data_.size() << std::endl;
    auto it_ptr = iterator();
    auto it = it_ptr.get();
    int64_t previous = -1;
    while(it->hasNext()){
        int64_t current = it->next().first;
        if(previous >= current){
            std::cout << "[Validate] ERROR. Previous: " << previous << " >= " << current << std::endl;
            throw std::runtime_error("Validation error");
        }
        previous = current;
    }
}

/*****************************************************************************
 *                                                                           *
 *  Actual implementation                                                    *
 *                                                                           *
 *****************************************************************************/

std::pair<int64_t, int64_t> PMA::find_interval(int64_t min, int64_t max) const {
    int64_t start = locate_segment(min);
    while(element_exist_[start] && data_[start].first < min) start++;
    int64_t end = locate_segment(max);
    int64_t sz = data_.size();
    while(end < sz && (!element_exist_[end] || data_[end].first <= max)) end++;

    COUT_DEBUG("min: " << min << ", max: " << max << ", start: " << start << ", end: " << end << ", sz: " << sz << ", "
            "actual size: " << data_.size() << " actual start: " << locate_segment(min) << ", actual end: " << locate_segment(max));

    return std::pair<int64_t, int64_t>{ start, end };
}


static int fls(uint64_t x); // some kind of helper routine

void PMA::recalculate_density(){
   lower_element_.resize((size_t) tree_height_ + 1);
   upper_element_.resize((size_t) tree_height_ + 1);
   int level_length = segment_length_;

   for (int i = 0; i <= tree_height_; i++) {
       double density_lower = density_lower_thres_root_ +
                              (density_lower_thres_leaf_ - density_lower_thres_root_) * (tree_height_ - i) /
                              tree_height_;
       double density_upper = density_upper_thres_root_ +
                              (density_upper_thres_leaf_ - density_upper_thres_root_) * (tree_height_ - i) /
                              tree_height_;

       lower_element_[i] = (int64_t) ceil(density_lower * level_length);
       upper_element_[i] = (int64_t) floor(density_upper * level_length);

       //special trim for wrong threshold introduced by float-integer conversion
       if (0 < i) {
           lower_element_[i] = std::max(lower_element_[i], 2 * lower_element_[i - 1]);
           upper_element_[i] = std::min(upper_element_[i], 2 * upper_element_[i - 1]);
       }
       level_length <<= 1;
   }
}

void PMA::init_pma(){
       // these four density threshold are the monotonically increasing
       assert(density_lower_thres_leaf_ < density_lower_thres_root_);
       assert(density_lower_thres_root_ < density_upper_thres_root_);
       assert(density_upper_thres_root_ < density_upper_thres_leaf_);

       // 2 * lower should be not greater than upper
       assert(2 * density_lower_thres_root_ <= density_upper_thres_root_);

       // the minimal tree structure has 2 levels with 4 elements' space, and the leaf segment's length is 2
       // even if the current density doesn't satisfy the minimum, a halving shouldn't be triggered
       this->segment_length_ = 2;
       this->tree_height_ = 1;
       this->recalculate_density();
       this->data_.resize(4);
       this->element_exist_.resize(4);
       this->element_cnt_ = 0;
}

int64_t PMA::locate_segment(int64_t key) const { // value
    // when current tree structure is minimal, the lower density is not guaranteed
    // special judgement is required
    if (4 == data_.size()) {
        if (element_exist_[2] && data_[2].first <= key) return 2;
        else return 0;
    }

    // binary search the appropriate segment for current value
    int64_t prefix = 0;
    int64_t current_bit = segment_length_ << tree_height_ >> 1;
    while (segment_length_ <= current_bit) {
        if (data_[prefix | current_bit].first <= key) prefix |= current_bit;
        current_bit >>= 1;
    }
    return prefix;
}

void PMA::project_buffer(int64_t head, int64_t rear) {
    buffer_.clear();
    for (auto i = head; i != rear; i++) {
        if (element_exist_[i]) buffer_.push_back(data_[i]);
    }
}

void PMA::evenly_dispatch_buffer(int64_t head, int64_t rear) {
    // reset exist flags
    std::fill(element_exist_.begin() + head, element_exist_.begin() + rear, false); // from <algorithm>

    int64_t node_length = rear - head;
    int64_t element_per_segment = (int64_t) buffer_.size() * segment_length_ / node_length;
    int64_t remainder = (int64_t) buffer_.size() * segment_length_ % node_length;

    // use remainder to handle the aliquant part
    // for each segment, if (cur_remainder + remainder) > node_length then assign one more to this segment
    // and update the new cur_remainder
    int64_t cur_remainder = 0;
    int64_t element_assigned = 0;
    int64_t cur_assigned_ptr = 0;
    for (auto i = head; i != rear; i += segment_length_) {
        cur_remainder += remainder;
        element_assigned = cur_remainder < node_length ? element_per_segment : element_per_segment + 1;
        cur_remainder %= node_length;
        for (auto j = i; j != i + element_assigned; j++) {
            element_exist_[j] = true;
            data_[j] = buffer_[cur_assigned_ptr++];
        }
    }
}

int64_t PMA::get_parent(int64_t left_location, int level) const {
    return left_location & ~(segment_length_ << level);
}

void PMA::resize(int64_t size) {
    project_buffer(0, (int64_t) data_.size());

    // fls is a builtin func for most x86 amd amd architecture
    // fls(int x) -> floor(log2(x)) + 1, which means the higher bit's index
    // segment length should be a pow of 2
    segment_length_ = 1 << (fls(fls(size)) - 1);
    tree_height_ = fls(size / segment_length_) - 1;

    // rebuild PMA
    recalculate_density();

    data_.resize((size_t) size);
    element_exist_.resize((size_t) size);
    evenly_dispatch_buffer(0, size);
}

void PMA::rebalance(int64_t left_location, int level){

    int node_length = segment_length_ << level;
    int64_t node_element_cnt = (int64_t) count(element_exist_.begin() + left_location,
            element_exist_.begin() + left_location + node_length, true); // from std::algorithm

    if (lower_element_[level] <= node_element_cnt && node_element_cnt <= upper_element_[level]) {
        // this node satisfy the desnity threshold, do the rebalance
        int64_t right_location = left_location + node_length;
        project_buffer(left_location, right_location);
        evenly_dispatch_buffer(left_location, right_location);
    } else {
        if (level == tree_height_) {
            // root imbalance, double or halve PMA
            if (node_element_cnt < lower_element_[level]) resize((int64_t) data_.size() >> 1);
            else resize((int64_t) data_.size() << 1);
        } else {
            // unsatisfied density, to rebalance its parent
            rebalance(get_parent(left_location, level), level + 1);
        }
    }
}

void PMA::insert_pma(ELEMENT_TYPE element) {
    int64_t segment_head = locate_segment(element.first);
    int64_t segment_size = (int64_t) count(element_exist_.begin() + segment_head,
                                   element_exist_.begin() + segment_head + segment_length_, true);
    int64_t segment_rear = segment_head + segment_size;

    for (int64_t i = segment_head; i != segment_rear; i++) {
        if (element.first < data_[i].first) swap(element, data_[i]);
    }
    element_exist_[segment_rear] = true;
    data_[segment_rear] = element;

    ++element_cnt_;
    ++segment_size;

    if (segment_size > upper_element_[0])
        rebalance(get_parent(segment_head, 0), 1);
}

static int fls(uint64_t x) {
//    int r = 32;
    if (!x) return 0;
//    if (!(x & 0xffff0000u)) {
//        x <<= 16;
//        r -= 16;
//    }
//    if (!(x & 0xff000000u)) {
//        x <<= 8;
//        r -= 8;
//    }
//    if (!(x & 0xf0000000u)) {
//        x <<= 4;
//        r -= 4;
//    }
//    if (!(x & 0xc0000000u)) {
//        x <<= 2;
//        r -= 2;
//    }
//    if (!(x & 0x80000000u)) {
//        x <<= 1;
//        r -= 1;
//    }
//    return r;

    static_assert(sizeof(unsigned long long) == 8, "Wrong built-in");
    return (64 - __builtin_clzll(x));
}

}} // namespace pma::sha
