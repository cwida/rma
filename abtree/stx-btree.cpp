/*
 * stx-btree.cpp
 *
 *  Created on: 26 Nov 2017
 *      Author: Dean De Leo
 *
 *  This is actually a wrapper to the STX-Btree implementation by
 *  Timo Bingmann: https://github.com/bingmann/stx-btree/
 */

#include "stx-btree.hpp"

#include <functional>
#include <iostream>
#if defined(HAVE_LIBNUMA)
#include <numa.h> // to dump statistics about the memory usage
#include <numaif.h>
#include <sched.h>
#endif
#include <limits>
#include <vector>

#include "configuration.hpp"
#include "miscellaneous.hpp" // to_string_with_unit_suffix

using namespace std;

namespace abtree {

STXBtree::STXBtree() {
    minimum = numeric_limits<int64_t>::max();
}

STXBtree::~STXBtree() {
    if(config().verbose()){ dump_memory_usage(); }
}


void STXBtree::insert(int64_t key, int64_t value) {
    impl.insert2(key, value);
    if(key < minimum) minimum = key;
}

/**
 * Return the value associated to the element with the given `key', or -1 if not present.
 * In case of duplicates, it returns the value of one of the qualifying elements.
 */
int64_t STXBtree::find(int64_t key) const {
    auto res = impl.find(key);
    if ( res != impl.end() ){
        return res->second;
    } else {
        return -1;
    }
}

size_t STXBtree::size() const {
    return impl.size();
}

unique_ptr<pma::Iterator> STXBtree::iterator() const {
    return find(minimum, numeric_limits<int64_t>::max());
}

unique_ptr<pma::Iterator> STXBtree::find(int64_t min, int64_t max) const {
    if(impl.empty())
        return unique_ptr<pma::Iterator>{ new STXBtree::IteratorImpl{impl.end(), impl.end(), 0} };

    auto it = impl.find(min);
    return unique_ptr<pma::Iterator>{ new STXBtree::IteratorImpl{it, impl.end(), max} };
}

pma::Interface::SumResult STXBtree::sum(int64_t min, int64_t max) const {
    pma::Interface::SumResult result;
    if(impl.empty()) return result;
    auto it = impl.lower_bound(min); // first key >= min
    auto end = impl.upper_bound(max); // first key > max
    if(it == end) return result;
    result.m_first_key = it->first;
    while(it != end){
        result.m_sum_keys += it->first;
        result.m_sum_values += it->second;
        result.m_num_elements ++;
        ++it;
    }
    --it; // go back of one position to fetch the last key
    result.m_last_key = it->first;

    return result;
}

void STXBtree::dump() const {
    if(impl.empty()){
        cout << "STX BTree empty!" << endl;
        return;
    }

    cout << "STX BTree size: " << size() << ", index_B: " << btree_traits::innerslots << ", leaf_B: " << btree_traits::leafslots << endl;
    auto index = 0;
    auto it = iterator();
    while(it->hasNext()){
        auto p = it->next();
        cout << "[" << index << "] key: " << p.first << ", value: " << p.second << endl;
        index++;
    }
}

void STXBtree::dump_memory_usage() const {
    cout << "[Memory statistics] cardinality: " << size();

    auto& statistics = impl.get_stats();
    auto mem_inodes = statistics.innernodes * sizeof(decltype(impl)::inner_node);
    cout << ", inodes: " << statistics.innernodes << ", (" << to_string_with_unit_suffix(mem_inodes) << ")";
    auto mem_leaves = statistics.leaves * sizeof(decltype(impl)::leaf_node);
    cout << ", leaves: " << statistics.leaves << ", (" << to_string_with_unit_suffix(mem_leaves) << ")";
    cout << ", total: " << (statistics.innernodes + statistics.leaves) << ", (" << to_string_with_unit_suffix(mem_inodes + mem_leaves) << ")";
    cout << endl;

#if defined(HAVE_LIBNUMA)
    if(numa_available() != -1){
        auto current_cpu = sched_getcpu();
        auto current_node = numa_node_of_cpu(current_cpu);
        auto tot_nodes = numa_num_configured_nodes();

        cout << "--> Executing on cpu: " << current_cpu << ", node: " << current_node << ", total nodes: " << tot_nodes << "\n";
        if(tot_nodes > 1){
            // map the allocations of nodes
            vector<int> map_inodes(tot_nodes);
            vector<int> map_leaves(tot_nodes);

            using node_t = decltype(impl)::node;
            function<void(node_t*)> explore;
            explore = [&map_inodes, &map_leaves, &explore](node_t* node){
                bool is_leaf = node->isleafnode();

                // dfs on the children
                if(!is_leaf){
                    auto inode = reinterpret_cast<decltype(impl)::inner_node*>(node);
                    for(size_t i = 0; i <= inode->slotuse; i++){
                        explore(inode->childid[i]);
                    }
                }

                int numa_node {-1};
                auto rc = get_mempolicy(&numa_node, nullptr, /* ignored*/ 0, node, MPOL_F_NODE | MPOL_F_ADDR);
                if( rc != 0 ){
                    std::cerr << "[STXBtree::dump_memory_usage] get_mempolicy: " << strerror(errno) << " (" << errno << "), logical node: " << node << std::endl;
                } else if( numa_node < 0 || numa_node >= (int) map_inodes.size() ){
                    std::cerr << "[STXBtree::dump_memory_usage] get_mempolicy, invalid result: " << numa_node << ", logical node: " << node << std::endl;
                } else if(is_leaf) {
                    map_leaves[numa_node]++;
                } else {
                    map_inodes[numa_node]++;
                }
            };

            explore(impl.m_root);

            for(size_t i = 0; i < map_inodes.size(); i++){
                auto mem_inodes = map_inodes[i] * sizeof(decltype(impl)::inner_node);
                auto mem_leaves = map_leaves[i] * sizeof(decltype(impl)::leaf_node);
                cout << "--> Node: " << i;
                cout << ", inodes: " << map_inodes[i] << " (" << to_string_with_unit_suffix(mem_inodes) << ")";
                cout << ", leaves: " << map_leaves[i] << " (" << to_string_with_unit_suffix(mem_leaves) << ")";
                cout << ", total: " << (map_inodes[i] + map_leaves[i]) << " (" << to_string_with_unit_suffix(mem_inodes + mem_leaves) << ")";
                cout << "\n";
            }
        }
    }

#endif

}

STXBtree::IteratorImpl::IteratorImpl(const btree_t::const_iterator& iterator, const btree_t::const_iterator& end, int64_t max) :
        iterator(iterator), end(end), max(max){

}

bool STXBtree::IteratorImpl::hasNext() const {
    return iterator != end && iterator->first <= max;
}

pair<int64_t, int64_t> STXBtree::IteratorImpl::next() {
    pair<int64_t, int64_t> p = *iterator; // make a copy of key/value here
    ++iterator;
    return p;
}

} // namespace abtree

