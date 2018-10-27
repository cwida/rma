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

#include "abtree.hpp"

#include <algorithm>
#include <cassert>
#include <cmath> // log
#include <cstddef>
#include <cstdlib> // posix_memalign
#include <cstdio> // snprintf
#include <cstring> // memcpy
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits> // std::numeric_limits<int64_t>
#include <memory>
#if defined(HAVE_LIBNUMA)
#include <numa.h> // to dump statistics about the memory usage
#include <numaif.h>
#include <sched.h>
#endif
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "database.hpp"
#include "miscellaneous.hpp" // to_string_with_unit_suffix
#include "timer.hpp" // Time the single PANN operations
#include "distribution/random_permutation.hpp"

using namespace distribution; // RandomPermutation

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#if defined(DEBUG)
    #define COUT_DEBUG(msg) std::cout << "[ABTree::" << __FUNCTION__ << "] " << msg << std::endl
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

namespace abtree {

ABTree::ABTree(size_t B) : ABTree(B/2, B) { }

ABTree::ABTree(size_t A, size_t B) : ABTree(A, B, A, B) { }

ABTree::ABTree(size_t iA, size_t iB, size_t lA, size_t lB) :
    intnode_a(iA), intnode_b(iB), leaf_a(lA), leaf_b(lB), min_sizeof_inode(init_memsize_internal_node()), min_sizeof_leaf(init_memsize_leaf()), root(create_leaf()), cardinality(0), num_nodes_allocated(0), num_leaves_allocated(1) {
    validate_bounds();

    // Use the same memory space for nodes & leaves ?
    bool abtree_random_permutation = false;
    try { // driver::initialize() should have been already called
        ARGREF(bool, "abtree_random_permutation").get(abtree_random_permutation);
    } catch ( configuration::ConsoleArgumentError& e ) { } // ignore
    if(abtree_random_permutation) set_common_memsize_nodes(true);
}

ABTree::ABTree(size_t B, std::pair<int64_t,int64_t>* elements, std::size_t elements_sz) : ABTree(B/2, B, elements, elements_sz) { }

ABTree::ABTree(size_t A, size_t B, std::pair<int64_t,int64_t>* elements, std::size_t elements_sz) :
        ABTree(A, B, A, B, elements, elements_sz) { }

ABTree::ABTree(size_t iA, size_t iB, size_t lA, size_t lB, std::pair<int64_t,int64_t>* elements, std::size_t elements_sz) :
        intnode_a(iA), intnode_b(iB), leaf_a(lA), leaf_b(lB), min_sizeof_inode(init_memsize_internal_node()), min_sizeof_leaf(init_memsize_leaf()), root(nullptr), cardinality(0), num_nodes_allocated(0), num_leaves_allocated(0){
    validate_bounds();

    // Use the same memory space for nodes & leaves ?
    bool abtree_random_permutation = false;
    try { // driver::initialize() should have been already called
        ARGREF(bool, "abtree_random_permutation").get(abtree_random_permutation);
    } catch ( configuration::ConsoleArgumentError& e ) { } // ignore
    if(abtree_random_permutation) set_common_memsize_nodes(true);

    // Load the elements
    initialize_from_array(elements, elements_sz);
}

ABTree::~ABTree(){
    if(config().verbose()){ dump_memory(std::cout); }

    if(record_leaf_statistics) { record_leaf_distance_memory(); }

    delete_node(root, 0);
    root = nullptr;
}

void ABTree::initialize_from_array(std::pair<int64_t, int64_t>* elements, size_t size, bool do_sort){
    using namespace std;

    // sort the elements in the input array
    if(do_sort){
        auto comparator = [](const pair<int64_t, int64_t>& e1, const pair<int64_t, int64_t>& e2){
            return e1.first < e2.first;
        };
        std::sort(elements, elements + size, comparator);
    }

    assert(root == nullptr);
    // single root?
    if(size <= leaf_b){
        Leaf* leaf = create_leaf();
        for(size_t i = 0; i < size; i++){
            KEYS(leaf)[i] = elements[i].first;
            VALUES(leaf)[i] = elements[i].second;
        }
        leaf->N = cardinality = size;

        root = leaf;
        return;
    }

    // general case
    // leaves
    const size_t num_leaves = ceil(static_cast<double>(size) / leaf_b);
    const size_t leaf_ff = size / num_leaves;
    const size_t leaf_ff_mod = size % num_leaves;
    size_t leaf_index = 0; // in [0, leaf_ff), or leaf_ff+1) for the first leaves
    size_t leaf_bbpoint = leaf_ff_mod * (leaf_ff + 1);
    Leaf* leaf_current = create_leaf();

    // inner nodes
    int64_t num_levels = ceil(log(num_leaves) / log(intnode_b));
    struct NodeState{
        InternalNode* node = nullptr;
        int64_t first_key = 0; /* it doesn't matter: we don't store the leftmost min anywhere */
        size_t fill_factor = 0;
        size_t index = 0;
        size_t bbpoint = 0;
    };
    vector<NodeState> levels(num_levels);
    height = num_levels + 1;

    // compute the fill factor & bbpoint
    size_t num_children = num_leaves;
    for(int l = num_levels -1; l > 0; l--){
        size_t num_nodes = ceil(((double) num_children) / intnode_b);
        size_t ff = num_children / num_nodes;
        size_t mod = num_children % num_nodes;
        levels[l].fill_factor = ff;
        levels[l].bbpoint = mod;

        num_children = num_nodes;
    }
    levels[0].fill_factor = intnode_b;
    levels[0].bbpoint = 1;

    std::function<void(int,Node*,int64_t,bool)> propagate;
    propagate = [&propagate, this, &levels](int level, Node* child, int64_t key, bool last){
        assert(level >= 0);

        NodeState& current = levels[level];
        if(!current.node ||
                (current.index < current.bbpoint && current.node->N == current.fill_factor +1) ||
                (current.index >= current.bbpoint && current.node->N == current.fill_factor)){
            assert(((current.node == nullptr && level == 0) || last == false) &&
                    "It implies creating a new node with no key at the last iteration");
            assert((current.node == nullptr || level > 0) && "Are we splitting the root?!");
            if(current.node != nullptr){
                propagate(level -1, current.node, current.first_key, last);
                current.index++;
            }
            current.node = create_internal_node();
            CHILDREN(current.node)[0] = child;
            current.node->N = 1; // node->N refers to the # pointers contained!
            current.first_key = key;

        } else {
            assert(current.node->N > 0 && current.node->N < intnode_b);
            CHILDREN(current.node)[current.node->N] = child;
            KEYS(current.node)[current.node->N -1] = key;
            current.node->N++;
            if(last && level > 0)
                propagate(level -1, current.node, current.first_key, last);
        }
    };

    for(size_t i = 0; i < size; i++){
        // next leaf?
        if((i < leaf_bbpoint && leaf_index > leaf_ff) || (i >= leaf_bbpoint && leaf_index >= leaf_ff)){
            leaf_current->N = leaf_ff;
            if(i <= leaf_bbpoint) leaf_current->N++; // leaf_current refers to the previous leaf here!

            propagate(num_levels -1, leaf_current, KEYS(leaf_current)[0], false);

            Leaf* leaf_next = create_leaf();
            leaf_next->previous = leaf_current;
            leaf_current->next = leaf_next;
            leaf_current = leaf_next;
            leaf_index = 0;
        }

        KEYS(leaf_current)[leaf_index] = elements[i].first;
        VALUES(leaf_current)[leaf_index] = elements[i].second;

        leaf_index++;
    }
    leaf_current->N = leaf_ff; // update the size for the last leaf
    propagate(num_levels -1, leaf_current, KEYS(leaf_current)[0], true); // attach the last leaf
    root = levels[0].node;
    cardinality = size;
}


void ABTree::load(const std::pair<int64_t, int64_t>* elements, size_t elements_sz){
    assert(cardinality == 0 && "Expected empty");
    if(cardinality > 0){ throw std::runtime_error("This method can only be invoked when the data structure is empty"); }

    assert(root != nullptr);
    assert(root->N == 0);
    delete_node(root, 0); root = nullptr;

    // check whether the elements are already sorted
    bool sorted = true;
    size_t i = 1;
    while(i < elements_sz && sorted){
        sorted = (elements[i -1].first <= elements[i].first);
        i++;
    }

    if(sorted){
        std::pair<int64_t, int64_t>* const_elements = const_cast<std::pair<int64_t, int64_t>*>(elements);
        initialize_from_array(const_elements, elements_sz, false);
    } else {
        std::unique_ptr<std::pair<int64_t, int64_t>[]> elements_ptr
            { new std::pair<int64_t, int64_t>[elements_sz] };
        memcpy(elements_ptr.get(), elements, sizeof(elements[0]) * elements_sz);
        initialize_from_array(elements_ptr.get(), elements_sz, true);
    }
}

/*****************************************************************************
 *                                                                           *
 *   Node properties                                                         *
 *                                                                           *
 *****************************************************************************/
bool ABTree::Node::empty() const {
    return N == 0;
}

int64_t* ABTree::KEYS(const InternalNode* inode) const{
    InternalNode* instance = const_cast<InternalNode*>(inode);
    return reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(instance) + sizeof(InternalNode));
}

ABTree::Node** ABTree::CHILDREN(const InternalNode* inode) const {
    return reinterpret_cast<Node**>(KEYS(inode) + intnode_b);
}

int64_t* ABTree::KEYS(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}

int64_t* ABTree::VALUES(const Leaf* leaf) const {
    return KEYS(leaf) + leaf_b;
}


/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/

size_t ABTree::size() const{
    return cardinality;
}

bool ABTree::is_leaf(int depth) const {
    assert(depth < height);
    return (depth == height -1);
}

ABTree::InternalNode* ABTree::create_internal_node() const {
    static_assert(!std::is_polymorphic<InternalNode>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(InternalNode) == 8, "Expected only 8 bytes for the cardinality");

    // (cardinality) 1 + (keys=) intnode_b + (pointers) intnode_b +1 == 2 * intnode_b +2;
    InternalNode* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_internal_node());
    if(rc != 0) throw std::runtime_error("ABTree::create_internal_node, cannot obtain a chunk of aligned memory");
    ptr->N = 0;

    num_nodes_allocated++;
    return ptr;
}

size_t ABTree::init_memsize_internal_node() const {
    return sizeof(InternalNode) + sizeof(int64_t) * (2 * intnode_b +1);
}

size_t ABTree::memsize_internal_node() const {
    if (common_memsize){
        return std::max(min_sizeof_inode, min_sizeof_leaf);
    } else {
        return min_sizeof_inode;
    }
}

ABTree::Leaf* ABTree::create_leaf() const {
    static_assert(!std::is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + ptr previous + ptr next");

    // (cardinality) 1 + (ptr left/right) 2 + (keys=) leaf_b + (values) leaf_b == 2 * leaf_b + 1;
    Leaf* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_leaf());
    if(rc != 0) throw std::runtime_error("ABTree::create_leaf, cannot obtain a chunk of aligned memory");
    ptr->N = 0;
    ptr->next = ptr->previous = nullptr;

    num_leaves_allocated++;
    return ptr;
}


size_t ABTree::init_memsize_leaf() const {
    return sizeof(Leaf) + sizeof(int64_t) * (2 * leaf_b);
}
size_t ABTree::memsize_leaf() const {
    if (common_memsize){
        return std::max(min_sizeof_inode, min_sizeof_leaf);
    } else {
        return min_sizeof_leaf;
    }
}

void ABTree::delete_node(Node* node, int depth) const {
    assert(node != nullptr);
    bool is_leaf = (depth == height -1);

    if(!is_leaf){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);

        for(size_t i = 0; i < inode->N; i++){
            delete_node(children[i], depth +1);
        }

        num_nodes_allocated--;
    } else {
        num_leaves_allocated--;
    }

    free(node);
}

#define _STR(x) #x
#define _ASSERT(x) if(!(x)) { throw std::runtime_error("[ABTree::validate_bounds] Assertion failed: " _STR(x)); }

void ABTree::validate_bounds() const {
    _ASSERT(intnode_a > 1);
    _ASSERT(2*intnode_a -1 <= intnode_b);
    _ASSERT(leaf_a > 1);
    _ASSERT(2*leaf_a -1 <= leaf_b);
}

#undef _STR
#undef _ASSERT

size_t ABTree::get_lowerbound(int depth) const {
    bool is_leaf = (depth == height -1);
    return is_leaf ? leaf_a : intnode_a;
}
size_t ABTree::get_upperbound(int depth) const {
    bool is_leaf = (depth == height -1);
    return is_leaf ? leaf_b : intnode_b;
}

void ABTree::validate_bounds(const Node* node, int depth) const {
  if(!node) return;
  assert(node->N >= get_lowerbound(depth));
  assert(node->N <= get_upperbound(depth));
}

void ABTree::validate() const{ validate_rec(root, 0); }

void ABTree::validate_rec(const Node* node, int depth) const{
    assert(node != nullptr);
    bool is_leaf = (depth == height -1);

    if (is_leaf){
        if(node == root){
            if(node->N > leaf_b){
                throw std::range_error("BTree#validate");
            }
        } else {
            if(node->N < leaf_a || node->N > leaf_b){
                throw std::range_error("BTree#validate");
            }
        }
    } else {
        if (node == root){
            if(node->N == 0 || node->N > intnode_b){
                throw std::range_error("BTree#validate");
            }
        } else {
            if(node->N < intnode_a || node->N > intnode_b){
                throw std::range_error("BTree#validate");
            }
        }
        auto inode = reinterpret_cast<const InternalNode*>(node);
        for(size_t i = 0; i < inode->N; i++){
            validate_rec(CHILDREN(inode)[i], depth +1);
        }
    }
}

size_t ABTree::memory_footprint() const {
    return num_nodes_allocated * memsize_internal_node() + num_leaves_allocated * memsize_leaf();
}

int64_t ABTree::key_max() const {
    if(cardinality == 0) return -1;
    Node* node = root;
    assert(node != nullptr);

    for(int depth = 0, l = height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        node = CHILDREN(inode)[inode->N -1];
    }

    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0 && "Empty leaf");
    return KEYS(leaf)[leaf->N -1];
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/

void ABTree::split_root(){
    InternalNode* root0 = create_internal_node();
    CHILDREN(root0)[0] = root;
    root0->N = 1;
    height++;
    split(root0, 0, 1);
    root = root0;
}

void ABTree::split(InternalNode* inode, size_t child_index, int child_depth){
    assert(inode != nullptr);
    assert(child_index <= inode->N);
    COUT_DEBUG("inode: " << inode << ", child_index: " << child_index << ", child_depth: " << child_depth);

    bool child_is_leaf = child_depth >= height -1;
    int64_t pivot = -1;
    Node* ptr = nullptr; // the new child

//    std::cout << "[ABTree::split] inode: " << inode << ", child_index: " << child_index << ", child_depth: " << child_depth << ", is_leaf: " << child_is_leaf << std::endl;

    if(child_is_leaf){
        // split a leaf in half
        Leaf* l1 = reinterpret_cast<Leaf*>(CHILDREN(inode)[child_index]);
        Leaf* l2 = create_leaf();

        assert(l1->N <= leaf_b);

        size_t thres = (l1->N +1) /2;
        l2->N = l1->N - thres;
        assert(l2->N >= leaf_a);
        l1->N = thres;
        assert(l1->N >= leaf_a);

        // move the elements from l1 to l2
        memcpy(KEYS(l2), KEYS(l1) + thres, l2->N * sizeof(KEYS(l2)[0]));
        memcpy(VALUES(l2), VALUES(l1) + thres, l2->N * sizeof(VALUES(l2)[0]));

        // adjust the links
        l2->next = l1->next;
        if( l2->next != nullptr ) { l2->next->previous = l2; }
        l2->previous = l1;
        l1->next = l2;

        // threshold derives the new pivot
        pivot = KEYS(l2)[0]; // == l1->keys[thres]
        ptr = l2;
    }

    // split an internal node
    else {
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(inode)[child_index]);
        InternalNode* n2 = create_internal_node();

        size_t thres = n1->N /2;
        n2->N = n1->N - (thres +1);
        assert(n2->N >= intnode_a);
        n1->N = thres +1;
        assert(n1->N >= intnode_a);

        // move the elements from n1 to n2
        assert(n2->N > 0);
        memcpy(KEYS(n2), KEYS(n1) + thres + 1, (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(CHILDREN(n2), CHILDREN(n1) + thres + 1, n2->N * sizeof(CHILDREN(n2)[0]));

        // derive the new pivot
        pivot = KEYS(n1)[thres];
        ptr = n2;
    }

    // finally, add the pivot to the parent (current node)
    assert(inode->N <= intnode_b); // when inserting, the parent is allowed to become b+1
    int64_t* keys = KEYS(inode);
    Node** children = CHILDREN(inode);

    for(int64_t i = static_cast<int64_t>(inode->N) -1, child_index_signed = child_index; i > child_index_signed; i--){
        keys[i] = keys[i-1];
        children[i +1] = children[i];
    }

    keys[child_index] = pivot;
    children[child_index +1] = ptr;
    inode->N++;
}

void ABTree::insert(Node* node, int64_t key, int64_t value, int depth){
    assert(node != nullptr);
    COUT_DEBUG("Node: " << node << ", key: " << key << ", value: " << value << ", depth: " << depth);

    // tail recursion on the internal nodes
    while(depth < (height -1)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        assert(inode->N > 0);
        size_t i = 0, last_key = inode->N -1;
        int64_t* __restrict keys = KEYS(inode);
        while(i < last_key && key > keys[i]) i++;
        node = CHILDREN(inode)[i];
        COUT_DEBUG("inode: " << inode << ", depth: " << depth << "/" << height << ", child index: " << i);

        // before moving to its child, check whether it is full. If this is the case
        // we need to make a recursive call to check again whether we need to split the
        // node after an element has been inserted
        bool child_is_leaf = (depth + 1) >= height -1;
        if(child_is_leaf && node->N == leaf_b){
            split(inode, i, depth +1); // we already know we are going to insert an element
            if(key > KEYS(inode)[i]) node = CHILDREN(inode)[++i];
        } else if (!child_is_leaf && node->N == intnode_b){
            insert(node, key, value, depth+1);
            if(node->N > intnode_b){ split(inode, i, depth+1); }
            return; // stop the loop
        }

        depth++;
    }

    // finally, shift the elements & insert into the leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    COUT_DEBUG("leaf: " << leaf);
    assert(leaf->N < leaf_b);
    size_t i = leaf->N;
    int64_t* __restrict keys = KEYS(leaf);
    int64_t* __restrict values = VALUES(leaf);
    while(i > 0 && keys[i-1] > key){
        keys[i] = keys[i-1];
        values[i] = values[i-1];
        i--;
    }
    keys[i] = key;
    values[i] = value;
    leaf->N++;

    cardinality += 1;
}

void ABTree::insert(int64_t key, int64_t value){
    // split the root when it is a leaf
    if(height == 1 && root->N == leaf_b){
        split_root();
    }

    insert(root, key, value, 0);

    // split the root when it is an internal node
    if(height > 1 && root->N > intnode_b){
        split_root();
    }

    sanity_check();
}

/*****************************************************************************
 *                                                                           *
 *   Remove (interval)                                                       *
 *                                                                           *
 *****************************************************************************/

void ABTree::merge(InternalNode* node, size_t child_index, int child_depth){
    assert(node != nullptr);
    assert(child_index +1 <= node->N);
    COUT_DEBUG("Node: " << node << ", child_index: " << child_index << ", child_depth: " << child_depth);

    // merge two adjacent leaves
    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];
        COUT_DEBUG("l1: " << l1 << ", l2: " << l2);
        assert(l1->N + l2->N <= leaf_b);

        // move all elements from l2 to l1
        memcpy(KEYS(l1) + l1->N, KEYS(l2), l2->N * sizeof(KEYS(l2)[0]));
        memcpy(VALUES(l1) + l1->N, VALUES(l2), l2->N * sizeof(VALUES(l2)[0]));

        // update the sizes of the two leaves
        l1->N += l2->N;
        l2->N = 0;

        // adjust the links
        l1->next = l2->next;
        if(l2->next != nullptr){ l2->next->previous = l1; }

        // free the memory from l2
        delete_node(l2, child_depth); l2 = nullptr;
    }

    // merge two adjacent internal nodes
    else {
        InternalNode* n1 = reinterpret_cast<InternalNode*>(CHILDREN(node)[child_index]);
        InternalNode* n2 = reinterpret_cast<InternalNode*>(CHILDREN(node)[child_index +1]);
        COUT_DEBUG("n1: " << n1 << ", n2: " << n2);
        assert(n1->N + n2->N + 1 <= intnode_b);

        // move the pivot into n1
        KEYS(n1)[n1->N -1] = KEYS(node)[child_index];
        CHILDREN(n1)[n1->N] = CHILDREN(n2)[0];

        // move all elements from n2 to n1 (except the first pointer from n2)
        assert(n2->N > 0);
        memcpy(KEYS(n1) + n1->N, KEYS(n2), (n2->N -1) * sizeof(KEYS(n2)[0]));
        memcpy(CHILDREN(n1) + n1->N +1, CHILDREN(n2) +1, (n2->N -1) * sizeof(CHILDREN(n2)[0]));

        // update the sizes of the two nodes
        n1->N += n2->N;
        n2->N = 0;

        // deallocate the intermediate node
        delete_node(n2, child_depth); n2 = nullptr;
    }

    // finally, remove the pivot from the parent (current node)
    assert(node->N >= get_lowerbound(child_depth -1) || node == root);
    // node->N might become |a-1|, this is still okay in a remove operation as we are
    // going to rebalance this node in post-order
    int64_t* keys = KEYS(node);
    Node** children = CHILDREN(node);
    for(size_t i = child_index +1, last = node->N -1; i < last; i++){
        keys[i -1] = keys[i];
        children[i] = children[i+1];
    }
    node->N--;
}

void ABTree::rotate_right(InternalNode* node, size_t child_index, int child_depth, size_t need){
    assert(node != nullptr);
    assert(0 < child_index && child_index < node->N);
    assert(need > 0);
    assert(CHILDREN(node)[child_index-1]->N >= need);
    assert(CHILDREN(node)[child_index]->N + need <= get_upperbound(child_depth));
    COUT_DEBUG("Node: " << node << ", child_index: " << child_index << ", child_depth: " << child_depth << ", need: " << need);

    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index -1];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index];

        int64_t* __restrict l1_keys = KEYS(l1);
        int64_t* __restrict l1_values = VALUES(l1);
        int64_t* __restrict l2_keys = KEYS(l2);
        int64_t* __restrict l2_values = VALUES(l2);

        // shift elements in l2 by `need'
        for(size_t i = l2->N -1 + need; i > 0; i--){
            l2_keys[i] = l2_keys[i - need];
            l2_values[i] = l2_values[i - need];
        }

        // copy `need' elements from l1 to l2
        for(size_t i = 0; i < need; i++){
            l2_keys[i] = l1_keys[l1->N - need +i];
            l2_values[i] = l1_values[l1->N - need +i];
        }
        // update the split point
        KEYS(node)[child_index -1] = l2_keys[0];

        // update the cardinalities
        l2->N += need;
        l1->N -= need;
    } else { // the children are internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index -1];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index];

        int64_t* __restrict n2_keys = KEYS(n2);
        Node** __restrict n2_children = CHILDREN(n2);
        int64_t* __restrict n1_keys = KEYS(n1);
        Node** __restrict n1_children = CHILDREN(n1);

        // shift elements in n2 by `need'
        if(n2->N > 0){
            n2_children[n2->N + need -1] = n2_children[n2->N -1];
            for(size_t i = n2->N + need -2; i >= need; i--){
                n2_keys[i] = n2_keys[i - need];
                n2_children[i] = n2_children[i - need];
            }
        }
        // move the pivot from node to n2
        n2_keys[need -1] = KEYS(node)[child_index-1];
        n2_children[need -1] = CHILDREN(n1)[n1->N -1];

        // copy the remaining elements from n1 to n2
        size_t idx = n1->N - need;
        for(size_t i = 0; i < need -1; i--){
            n2_keys[i] = n1_keys[idx];
            n2_children[i] = n1_children[idx];
            idx++;
        }

        // update the pivot
        KEYS(node)[child_index-1] = KEYS(n1)[n1->N - need -1];

        n2->N += need;
        n1->N -= need;
    }
}

void ABTree::rotate_left(InternalNode* node, size_t child_index, int child_depth, size_t need){
    assert(node != nullptr);
    assert(0 <= child_index && child_index < node->N);
    assert(CHILDREN(node)[child_index]->N + need <= get_upperbound(child_depth));
    assert(CHILDREN(node)[child_index+1]->N >= need);
    COUT_DEBUG("Node: " << node << ", child_index: " << child_index << ", child_depth: " << child_depth << ", need: " << need);

    if(is_leaf(child_depth)){
        Leaf* l1 = (Leaf*) CHILDREN(node)[child_index];
        Leaf* l2 = (Leaf*) CHILDREN(node)[child_index +1];

        int64_t* __restrict l1_keys = KEYS(l1);
        int64_t* __restrict l1_values = VALUES(l1);
        int64_t* __restrict l2_keys = KEYS(l2);
        int64_t* __restrict l2_values = VALUES(l2);

        // move `need' elements of l2 in l1
        for(size_t i = 0; i < need; i++){
            l1_keys[l1->N + i] = l2_keys[i];
            l1_values[l1->N + i] = l2_values[i];
        }

        // left shift elements by `need' in l2
        for(int64_t i = 0, sz = l2->N -need; i < sz; i++){
            l2_keys[i] = l2_keys[i+need];
            l2_values[i] = l2_values[i+need];
        }

        // update the pivot
        KEYS(node)[child_index] = l2_keys[0];

        // adjust the sizes
        l1->N += need;
        l2->N -= need;
    } else { // internal nodes
        InternalNode* n1 = (InternalNode*) CHILDREN(node)[child_index];
        InternalNode* n2 = (InternalNode*) CHILDREN(node)[child_index +1];
        COUT_DEBUG("n1: " << n1 << ", n2: " << n2);

        int64_t* __restrict n1_keys = KEYS(n1);
        Node** __restrict n1_children = CHILDREN(n1);
        int64_t* __restrict n2_keys = KEYS(n2);
        Node** __restrict n2_children = CHILDREN(n2);

        // add the pivot to n1
        assert(n1->N > 0);
        n1_keys[n1->N -1] = KEYS(node)[child_index];
        n1_children[n1->N] = n2_children[0];

        // move 'need -1' elements from n2 to n1
        size_t idx = n1->N;
        for(size_t i = 0; i < need -1; i++){
            n1_keys[idx] = n2_keys[i];
            n1_children[idx +1] = n2_children[i +1];
        }

        // update the pivot
        KEYS(node)[child_index] = n2_keys[need -1];

        // left shift elements by `need' in n2
        for(size_t i = 0, sz = n2->N -need -1; i < sz; i++){
            n2_keys[i] = n2_keys[i+need];
            n2_children[i] = n2_children[i+need];
        }
        n2_children[n2->N -need -1] = n2_children[n2->N -1];

        // adjust the sizes
        n1->N += need;
        n2->N -= need;
    }

}

void ABTree::rebalance_lb(InternalNode* node, size_t child_index, int child_depth){
    assert(node != nullptr);
    assert(node->N > 1 || node == root);
    assert(child_index < node->N);
    COUT_DEBUG("Node: " << node << ", child_index: " << child_index << ", child_depth: " << child_depth);

    // the child already contains more than a elements => nop
    size_t child_sz = CHILDREN(node)[child_index]->N;
    const size_t lowerbound = get_lowerbound(child_depth);
    if(child_sz >= lowerbound){ return; } // nothing to do!

    // okay, if this is the root && it has only one child, there is not much we can do
    if(node == root && node->N <= 1) return;

    // how many nodes do we need?
    int64_t need = lowerbound - child_sz;

    // check if we can steal `need' nodes from its sibling
    bool can_rotate_right = false;
    if(child_index > 0){ // steal from left
        Node* child_left = CHILDREN(node)[child_index -1];
        if(child_left->N >= lowerbound + need +1){
            rotate_right(node, child_index, child_depth, need +1);
            return; // done
        } else {
            can_rotate_right = child_left->N >= lowerbound + need;
        }
    }

    bool can_rotate_left = false;
    if(child_index < node->N -1){ // steal from right
        Node* child_right = CHILDREN(node)[child_index +1];
        if(child_right->N >= lowerbound + need +1){
            rotate_left(node, child_index, child_depth, need +1);
            return; // done
        } else {
            can_rotate_left = child_right->N >= lowerbound + need;
        }

    }
    // we cannot steal `need +1' nodes, but maybe we can rotate just `need' nodes
    // bringing the size of child to |a|
    if(can_rotate_right){
        rotate_right(node, child_index, child_depth, need);
        return;
    }
    if(can_rotate_left){
        rotate_left(node, child_index, child_depth, need);
        return;
    }

    // both siblings contain |a -1 + a| elements, merge the nodes
    if(child_index < node->N -1){
        merge(node, child_index, child_depth);

        validate_bounds(CHILDREN(node)[child_index], child_depth);
    } else {
        assert(child_index > 0);
        merge(node, child_index -1, child_depth);

        validate_bounds(CHILDREN(node)[child_index -1], child_depth);
    }
}

bool ABTree::reduce_tree(){
    bool result = false;

    while(height > 1 && root->N == 1){
        InternalNode* inode = reinterpret_cast<InternalNode*>(root);
        root = CHILDREN(inode)[0];
        inode->N = 0;
        delete_node(inode, 0);
        height--;

        result = true;
    }

    return result;
}

void ABTree::remove_subtrees_rec0(Node* node, int depth){
    if(node == nullptr) return;

    if(!is_leaf(depth)){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);
        for(size_t i = 0; i < inode->N; i++){
            remove_subtrees_rec0(children[i], depth +1);
            delete_node(children[i], depth +1);
            children[i] = nullptr;
        }
    } else {
        cardinality -= node->N;
    }

    node->N = 0;
}

void ABTree::remove_subtrees(InternalNode* node, size_t index, size_t length, int children_depth){
    assert(node != nullptr);
    assert(index + length <= node->N);

    int64_t* keys = KEYS(node);
    Node** children = CHILDREN(node);

    for(size_t i = index, last = index + length; i < last; i++){
        remove_subtrees_rec0(children[i], children_depth);
        delete_node(children[i], children_depth);
        children[i] = nullptr;
    }

    // if the length == node->N, then we are removing all elements
    assert(length < node->N || (index == 0 && node->N == length));
    if(length < node->N){
        // shift the pointers
        for(size_t i = index, last = node->N - length; i < last; i++){
            children[i] = children[i + length];
        }
        // shift the keys
        for(size_t i = (index > 0) ? index -1 : 0, last = node->N -1 - length; i < last; i++){
            keys[i] = keys[i + length];
        }
    }

    node->N -= length;
}

bool ABTree::remove_keys(Node* node, int64_t range_min, int64_t range_max, int depth, int64_t* min){
    if(!is_leaf(depth)){
        bool retrebalance = false;
        int64_t tmp (-1);
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t start = 0, N = inode->N;
        while(start < N -1 && KEYS(inode)[start] < range_min) start++;
        size_t end = start;
        while(end < N -1 && KEYS(inode)[end] <= range_max) end++;

        size_t remove_trees_start = start +1;
        size_t remove_trees_length = (end > start +1) ? end - start -1 : 0;

        // remove the keys at the head
        retrebalance |= remove_keys(CHILDREN(inode)[start], range_min, range_max, depth +1, min);
        if(CHILDREN(inode)[start]->empty()){
            remove_trees_start--;
            remove_trees_length++;
        }

        // remove the keys at the tail
        if(end > start){
            retrebalance |= remove_keys(CHILDREN(inode)[end], range_min, range_max, depth +1, &tmp);
            if(tmp == -1){ // empty block
                assert(CHILDREN(inode)[end]->empty());
                remove_trees_length++;
            } else {
                KEYS(inode)[end -1] = tmp;
            }
        }

        // remove whole trees
        if(remove_trees_length > 0){
            // before shifting the key containing the minimum for the next available block,
            // record into the variable *min
            if(min && start == 0){
                *min = (remove_trees_length < inode->N) ? KEYS(inode)[remove_trees_length -1] : -1;
            }

            remove_subtrees(inode, remove_trees_start, remove_trees_length, depth +1);
        }

        // when we remove from multiple children (i.e. start < end), we can have a situation like this:
        // inode->children: U | U | start = PR | FR | FR | FR | end = PR | U | U
        // where U = untouched, PR: subtree partially removed, FR: subtree wholly removed
        // then after remove_subtrees(...) is invoked, all FR branches are deleted from the array and the two PR
        // subtrees are next to each other. The next invocation ensures that only one of the two PRs has less |a| elements,
        // otherwise they are merged together.
        if(start < end && start+1 < inode->N && CHILDREN(inode)[start]->N + CHILDREN(inode)[start+1]->N <= 2* get_lowerbound(depth +1) -1){
            merge(inode, start, depth+1);
        }

        return retrebalance || inode->N < intnode_a;
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->N == 0) { // mmh
            if(min) *min = -1;
            return true;
        }

        int64_t* keys = KEYS(leaf);
        int64_t* values = VALUES(leaf);

        if((keys[0] <= range_max) && (keys[leaf->N -1] >= range_min)) {
            size_t start = 0;
            while(keys[start] < range_min) start++;
            size_t end = start;
            while(end < leaf->N && keys[end] <= range_max) end++;
            size_t length = end - start;
            size_t last = start + leaf->N - end;
            for(size_t i = start; i < last; i++){
                keys[i] = keys[i + length];
                values[i] = values[i + length];
            }
            leaf->N -= length;
            cardinality -= length;
        }

        if(min != nullptr){
            *min = leaf->N > 0 ? keys[0] : -1;
        }
        return leaf->N < leaf_a;
    }

}

void ABTree::rebalance_rec(Node* node, int64_t range_min, int64_t range_max, int depth){
    // base case
    if(is_leaf(depth)){
        assert(node->N >= leaf_a || node == root);
        return;
    }

    // rebalance the internal nodes
    InternalNode* inode = reinterpret_cast<InternalNode*>(node);
    int64_t* keys = KEYS(inode);
    Node** children = CHILDREN(inode);
    assert(inode->N > 0);
    size_t i = 0, inode_num_keys = inode->N -1;
    while(i < inode_num_keys && keys[i] < range_min) i++;

    rebalance_lb(inode, i, depth +1); // the first call ensures inode[i] >= |a+1| if possible, otherwise inode[i] >= |a|

    // if this is the root, check whether we need to reduce the tree if it has only one child
    if(node == root){
        bool reduced = reduce_tree(); // reduced is the same as checking root != node
        if(reduced){ return rebalance_rec(root, range_min, range_max, 0); }
    }

    rebalance_rec(children[i], range_min, range_max, depth +1);

    rebalance_lb(inode, i, depth +1); // the second time, it brings inode[i] from |a-1| to at least |a|

    // if this is the root, check whether we need to reduce the tree if it has only one child
    if(node == root && reduce_tree()){ return rebalance_rec(root, range_min, range_max, 0); }

    if(inode->N > 1 && i < inode->N -2 && keys[i] < range_max){
        rebalance_lb(inode, i +1, depth +1);
        if(node == root && reduce_tree()){ return rebalance_rec(root, range_min, range_max, 0); }
        rebalance_rec(children[i+1], range_min, range_max, depth +1);
        rebalance_lb(inode, i +1, depth +1); // ensure inode[i+1] >= |a|
        if(node == root && reduce_tree()){ return rebalance_rec(root, range_min, range_max, 0); }
    }
}

void ABTree::remove(Node* node, int64_t keymin, int64_t keymax, int depth){
    // first pass, remove the elements
    bool rebalance = remove_keys(node, keymin, keymax, depth, nullptr);

    if(rebalance){
        // quite an edge case, everything was removed from the tree, also the starting leaf, due to the subtree removal
        if(node == root && node->N == 0 && height > 1){
            delete_node(root, 0);
            height =1;
            root = create_leaf();
        } else {
            // standard case
            rebalance_rec(node, keymin, keymax, 0);
        }
    }
}

void ABTree::remove(int64_t min, int64_t max){
    remove(root, min, max, 0);
}

/*****************************************************************************
 *                                                                           *
 *   Remove (single element)                                                 *
 *                                                                           *
 *****************************************************************************/

int64_t ABTree::remove(Node* node, int64_t key, int depth, int64_t* omin){
    assert(node != nullptr);
    COUT_DEBUG("Remove key: " << key << ", at depth: " << depth << "/" << height);

    while(depth < height -1){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = node->N;
        assert(N > 0);
        while(i < N -1 && KEYS(inode)[i] < key) i++;
        if(omin == nullptr && i < N -1 && KEYS(inode)[i] == key){
            // in case omin != nullptr, then i = 0 and we are already following the min
            // from another internal node. This tree has many duplicates.
            // The resulting omin has to be inode->keys[i] = inode->keys[0] and
            // all keys from block 0 are equal to inode->keys[0];
            // Nevertheless we keep percolating the tree to remove the key from the leaf.
            COUT_DEBUG("Node " << inode << ", Case 1");
            int64_t result = -1; int64_t newkey = -1;
            result = remove(CHILDREN(inode)[i+1], key, depth +1, &newkey);
            KEYS(inode)[i] = newkey;
            rebalance_lb(inode, i+1, depth+1);
            return result; // stop the tail recursion
        } else if (CHILDREN(inode)[i]->N <= get_lowerbound(depth+1)){
            COUT_DEBUG("Node " << inode << ", Case 2");
            int64_t result = -1;
            result = remove(CHILDREN(inode)[i], key, depth+1, omin); // it might bring inode->pointers[i]->N == |a-1|
            rebalance_lb(inode, i, depth +1);
            return result; // stop the tail recursion
        } else { // the node has already |a+1| children, no need to rebalance
            COUT_DEBUG("Node " << inode << ", Case 3");
            node = CHILDREN(inode)[i];
        }

        depth++;
    }

    { // base case, this is a leaf
        COUT_DEBUG("Leaf: " << node << ", Base case");
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        size_t N = leaf->N;
        int64_t* keys = KEYS(leaf);
        int64_t* values = VALUES(leaf);
        int64_t value = -1;
        if(N > 0){
            if(keys[N-1] == key){
                value = values[N-1];
                leaf->N -= 1;
            } else if(keys[N-1] > key){
                size_t i = 0;
                while(i < N && keys[i] < key) i++;
                if(i < N && keys[i] == key){
                    value = values[i];
                    for(size_t j = i; j < leaf->N -1; j++){
                        keys[j] = keys[j+1];
                        values[j] = values[j+1];
                    }
                    leaf->N -= 1;
                }
            }
        }

        if(omin){
            if(leaf->N > 0){
                *omin = keys[0];
            } else {
                *omin = -1;
            }
        }
        return value;
    }

}

int64_t ABTree::remove(int64_t key) {
    COUT_DEBUG("key: " << key);

    int64_t value = remove(root, key, 0, nullptr);

    if(value >= 0){
        cardinality--;

        // shorten the tree
        if(root->N == 1 && height > 1){ // => the root is not a leaf
//            assert(height == 2);
            InternalNode* iroot = reinterpret_cast<InternalNode*>(root);
            root = CHILDREN(iroot)[0];

            iroot->N = 0;
            delete_node(iroot, 0);
            height--;
        }
    }

    sanity_check();
    return value;
}

/******************************************************************************
 *                                                                            *
 *   Find (single element)                                                    *
 *                                                                            *
 *****************************************************************************/

int64_t ABTree::find(int64_t key) const noexcept {
    Node* node = root; // start from the root
    assert(node != nullptr);

    // use tail recursion on the internal nodes
    for(int depth = 0, l = height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N -1;
        assert(N > 0 && N <= intnode_b);
        int64_t* __restrict keys = KEYS(inode);
        while(i < N && keys[i] <= key) i++;
        node = CHILDREN(inode)[i];
    }

    // base case, this is a leaf
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    size_t i = 0, N = leaf->N;
    int64_t* __restrict keys = KEYS(leaf);
    while(i < N && keys[i] < key) i++;
    return (i < N && keys[i] == key) ? VALUES(leaf)[i] : -1;
}

/******************************************************************************
 *                                                                            *
 *   Iterator                                                                 *
 *                                                                            *
 *****************************************************************************/
ABTree::Iterator::Iterator(const ABTree* tree, int64_t max, Leaf* block, int64_t pos): tree(tree), max(max), block(block), pos(pos) { }

bool ABTree::Iterator::hasNext() const { return block != nullptr; }

std::pair<int64_t, int64_t>  ABTree::Iterator::next() {
    if(!block) return std::pair<int64_t, int64_t>{-1, -1};

    auto v = std::make_pair(tree->KEYS(block)[pos], tree->VALUES(block)[pos]);

    // move to the next position
    if(pos >= block->N - 1){
        block = block->next;
        pos = 0;
    } else {
        pos++;
    }

    // is the next item satisfy the interval [min, max]?
    if(block && tree->KEYS(block)[pos] > max){
        block = nullptr;
    }

    return v;
}

std::unique_ptr<ABTree::Iterator> ABTree::create_iterator(int64_t max, Leaf* leaf, int64_t pos) const {
    if(leaf == nullptr || KEYS(leaf)[pos] > max){
        return std::unique_ptr<ABTree::Iterator>(new ABTree::Iterator(this, max, nullptr, 0));
    } else {
        return std::unique_ptr<ABTree::Iterator>(new ABTree::Iterator(this, max, leaf, pos));
    }
}

std::unique_ptr<ABTree::Iterator> ABTree::leaf_scan(Leaf* leaf, int64_t min, int64_t max) const {
    assert(leaf != nullptr);

    if(leaf->N == 0){ return std::unique_ptr<ABTree::Iterator>(new ABTree::Iterator(this, max, nullptr, 0)); }

    // edge case, the interval starts at the sibling leaf
    if(KEYS(leaf)[leaf->N -1] < min){
        leaf = leaf->next;
        if(leaf == nullptr) {
            return create_iterator(max, nullptr, 0);
        } else if (KEYS(leaf)[0] >= min){
            return create_iterator(max, leaf, 0);
        } else {
            return create_iterator(max, nullptr, 0);
        }

    // edge case, the interval should have started before this leaf
    } else if (KEYS(leaf)[0] > max){
        return create_iterator(max, nullptr, 0);

    // standard case, find the first key that satisfies the interval
    } else {
        size_t i = 0;
        while(i < leaf->N && KEYS(leaf)[i] < min) i++;
        return create_iterator(max, leaf, i);
    }
}

std::unique_ptr<pma::Iterator> ABTree::find(int64_t min, int64_t max) const {
    if(min > max) return create_iterator(max, nullptr, 0); // imp

    Node* node = root;

    for(int depth = 0, l = height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N;
        assert(N > 0);
        int64_t* __restrict keys = KEYS(inode);
        while(i < N -1 && keys[i] < min) i++;
        node = CHILDREN(inode)[i];
    }

    return leaf_scan(reinterpret_cast<Leaf*>(node), min, max);
}

/******************************************************************************
 *                                                                            *
 *   Sum interface                                                            *
 *                                                                            *
 *****************************************************************************/
pma::Interface::SumResult ABTree::sum(int64_t min, int64_t max) const {
    using SumResult = pma::Interface::SumResult;
    if(min > max || size() == 0){ return SumResult{}; }

    // Find the first leaf for the key `min'
    Node* node = root;
    for(int depth = 0, l = height -1; depth < l; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        size_t i = 0, N = inode->N;
        assert(N > 0);
        int64_t* __restrict keys = KEYS(inode);
        while(i < N -1 && keys[i] < min) i++;
        node = CHILDREN(inode)[i];
    }

    // First the first entry in the current leaf such that key >= min
    Leaf* leaf = reinterpret_cast<Leaf*>(node);
    assert(leaf->N > 0 && "Empty leaf");

    // edge case, the interval starts at the sibling leaf
    if(KEYS(leaf)[leaf->N -1] < min){
        leaf = leaf->next;
        assert((leaf == nullptr || leaf->N > 0) && "Empty leaf");
        if(leaf == nullptr || KEYS(leaf)[0] < min){
            return SumResult{};
        }
    }

    // edge case, the interval should have started before this leaf
    if (KEYS(leaf)[0] > max){ return SumResult{}; }

    // standard case, find the first key that satisfies the interval
    int64_t* __restrict keys = KEYS(leaf);
    int64_t* __restrict values = VALUES(leaf);
    int64_t i = 0;
    while(/*i < leaf->N && */ keys[i] < min) i++;

    int64_t N = leaf->N;
    SumResult result;
    result.m_first_key = keys[i];

    do {
        while(i < N && keys[i] <= max /* inclusive */){
            result.m_sum_keys += keys[i];
            result.m_sum_values += values[i];
            result.m_num_elements++;
            i++;
        }
        result.m_last_key = keys[i -1]; // just in case

        // move to the next leaf
        if(i >= N){
            leaf = leaf->next;
            if(leaf != nullptr){
                keys = KEYS(leaf);
                values = VALUES(leaf);
                i = 0;
                N = leaf->N;
                assert(N > 0 && "Empty leaf");
                if(keys[0] > max) leaf = nullptr;
            }
        } else {
            leaf = nullptr;
        }

        if(leaf != nullptr){
            // prefetch the next next leaf :!)
            PREFETCH(leaf->next);
            // prefetch the first two blocks for the keys
            PREFETCH(KEYS(leaf->next));
            PREFETCH(KEYS(leaf->next) + 8);
            // prefetch the first two blocks for the values
            PREFETCH(VALUES(leaf->next));
            PREFETCH(VALUES(leaf->next) + 8);
        }
    } while (leaf != nullptr);

    return result;
}
/******************************************************************************
 *                                                                            *
 *   Memory distance among the leaves                                         *
 *                                                                            *
 *****************************************************************************/
ABTree::LeafStatistics ABTree::get_stats_leaf_distance() const {
    LeafStatistics stats {1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    if(size() == 0) return stats; // empty

    // Reach the first leaf
    Node* node = root;
    for(int depth = 1; depth < height; depth++){
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        node = CHILDREN(inode)[0];
    }
    Leaf* current = reinterpret_cast<Leaf*>(node);
    assert(current != nullptr);
    auto current_last_element = reinterpret_cast<char*>(&(VALUES(current)[current->N -1]));

    stats.m_distance_min = std::numeric_limits<decltype(stats.m_distance_min)>::max();
    stats.m_cardinality_min = std::numeric_limits<decltype(stats.m_cardinality_min)>::max();

    std::vector<size_t> distances;
    size_t distance_sum = 0;
    size_t distance_sum_sq = 0;

    std::vector<size_t> cardinalities;
    size_t cardinality_sum = size();
    size_t cardinality_sum_sq = current->N * current->N;
    stats.m_cardinality_max = stats.m_cardinality_min = current->N;
    cardinalities.push_back(current->N);

    Leaf* next = current->next;
    while(next != nullptr){
        // memory distance
        size_t distance = std::min( std::abs(reinterpret_cast<char*>(next) - current_last_element),
                std::abs(reinterpret_cast<char*>(next) - reinterpret_cast<char*>(current)) );
        distances.push_back(distance);

        stats.m_num_leaves++;
        if(stats.m_distance_max < distance) stats.m_distance_max = distance;
        if(stats.m_distance_min > distance) stats.m_distance_min = distance;
        distance_sum += distance;
        distance_sum_sq += (distance * distance);

        // cardinality
        size_t sz = next->N;
        cardinality_sum_sq += sz * sz;
        if(stats.m_cardinality_max < sz) stats.m_cardinality_max = sz;
        if(stats.m_cardinality_min > sz) stats.m_cardinality_min = sz;
        cardinalities.push_back(sz);

        current = next;
        current_last_element = reinterpret_cast<char*>(&(VALUES(current)[current->N -1]));
        next = current->next;
    }

    // Memory distances
    // Compute avg & stddev
    if(stats.m_num_leaves > 1){
        stats.m_distance_avg = distance_sum / (stats.m_num_leaves -1);
        stats.m_distance_stddev = sqrt( (static_cast<double>(distance_sum_sq) / (stats.m_num_leaves -1)) -
                pow(stats.m_distance_avg, 2.0) );
        // Compute the median
        std::sort(begin(distances), end(distances));
        size_t dist_sz = distances.size();
        assert(dist_sz == stats.m_num_leaves -1);
        if(dist_sz % 2 == 1){
            stats.m_distance_median = distances[dist_sz /2];
        } else {
            size_t d1 = dist_sz /2;
            size_t d0 = d1 - 1;
            stats.m_distance_median = (distances[d0] + distances[d1]) / 2;
        }
    } else {
        stats.m_distance_avg = stats.m_distance_stddev = stats.m_distance_median = 0;
    }

    // Cardinalities
    stats.m_cardinality_avg = cardinality_sum / stats.m_num_leaves;
    stats.m_cardinality_stddev = sqrt( (static_cast<double>(cardinality_sum_sq) / stats.m_num_leaves) -
            pow(stats.m_cardinality_avg, 2.0) );
    std::sort(begin(cardinalities), end(cardinalities));
    size_t card_sz = cardinalities.size();
    assert(card_sz == stats.m_num_leaves);
    if(card_sz % 2 == 1){
        stats.m_cardinality_median = cardinalities[card_sz /2];
    } else {
        size_t d1 = card_sz /2;
        size_t d0 = d1 - 1;
        stats.m_cardinality_median = (cardinalities[d0] + cardinalities[d1]) / 2;
    }

    return stats;
}

void ABTree::record_leaf_distance_memory() const {
    LOG_VERBOSE("[ABTree] Computing the distances in memory among consecutive leaves...");

    auto stats = get_stats_leaf_distance();

    LOG_VERBOSE("--> # leaves: " << stats.m_num_leaves);
    LOG_VERBOSE("--> distance average: " << stats.m_distance_avg << ", min: " << stats.m_distance_min << ", max: " << stats.m_distance_max << ", std. dev: " <<
            stats.m_distance_stddev << ", median: " << stats.m_distance_median);
    LOG_VERBOSE("--> cardinality average: " << stats.m_cardinality_avg << ", min: " << stats.m_cardinality_min << ", max: " << stats.m_cardinality_max << ", std. dev: " <<
            stats.m_cardinality_stddev << ", median: " << stats.m_cardinality_median);

    config().db()->add("btree_leaf_statistics")
                    ("num_leaves", stats.m_num_leaves)
                    ("dist_avg", stats.m_distance_avg)
                    ("dist_min", stats.m_distance_min)
                    ("dist_max", stats.m_distance_max)
                    ("dist_stddev", stats.m_distance_stddev)
                    ("dist_median", stats.m_distance_median)
                    ("card_avg", stats.m_cardinality_avg)
                    ("card_min", stats.m_cardinality_min)
                    ("card_max", stats.m_cardinality_max)
                    ("card_stddev", stats.m_cardinality_stddev)
                    ("card_median", stats.m_cardinality_median)
                    ;
}

void ABTree::set_record_leaf_statistics(bool value) {
    record_leaf_statistics = value;
}

#if !defined(NDEBUG)
void ABTree::sanity_check(Node* node, int depth, int64_t minimum, std::unordered_map<Node*, bool>& map){
    assert(node != nullptr);
    assert(depth < height);
    assert(node->N >= get_lowerbound(depth) || node == root);
    assert(node->N <= get_upperbound(depth));
    assert(map.find(node) == map.end() && "Node already visited!");
    map[node] = true;

    if(is_leaf(depth)){
        Leaf* leaf = reinterpret_cast<Leaf*>(node);
        int64_t* keys = KEYS(leaf);
        for(size_t i = 0; i < node->N; i++){
            assert(keys[i] >= minimum);
            minimum = keys[i];
        }
    } else { // inner node
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);
        Node** children = CHILDREN(inode);
        int64_t* keys = KEYS(inode);

        // check the keys are non decreasing
        for(size_t i = 0; i < node->N; i++){
            sanity_check(children[i], depth+1, minimum, map);
            if(i > 0){
                assert(keys[i-1] >= minimum);
                minimum = keys[i-1];
            }
        }
    }
}

void ABTree::sanity_check(){
#if defined(DEBUG)
    std::unordered_map<Node*, bool> map;
    sanity_check(root, 0, std::numeric_limits<int64_t>::min(), map);
#endif
}
#endif

/******************************************************************************
 *                                                                            *
 *   Dump                                                                     *
 *                                                                            *
 *****************************************************************************/

void ABTree::dump_data(std::ostream& out, Node* node, int depth) const {
    using namespace std;
    assert(node != nullptr);
    assert(depth < height);

    const bool is_leaf = depth == height -1;

    // preamble
    auto flags = out.flags();
    if(depth > 0) out << ' ';
    out << setw(depth * 2) << setfill(' '); // initial padding
    out << "[" << setw(2) << setfill('0') << depth << "] ";
    out << (is_leaf ? "L" : "I") << ' ';
    out << hex << node << dec << " N: " << node->N << '\n' ;
    out.setf(flags);

    // tabs
    auto dump_tabs = [&out, depth](){
        auto flags = out.flags();
        out << setw(depth * 2 + 5) << setfill(' ') << ' ';
        out.setf(flags);
    };

    if (!is_leaf){ // internal node
        InternalNode* inode = reinterpret_cast<InternalNode*>(node);

        auto flags = out.flags();
        dump_tabs();
        out << "Keys: ";
        if(inode->N > 0){
            for(size_t i = 0; i < inode->N -1; i++){
                if(i > 0) out << ", ";
                out << i << ": " << KEYS(inode)[i];
            }
        }
        out << '\n';
        dump_tabs();
        out << "Ptrs: " << hex;
        for(size_t i = 0; i < inode->N; i++){
            if(i > 0) out << ", ";
            out << i << ": " << CHILDREN(inode)[i];
        }
        out << dec << endl;
        out.setf(flags);

        // recurse
        for(int i = 0, sz = inode->N; i < sz; i++){
            dump_data(out, CHILDREN(inode)[i], depth+1);
        }
    } else { // this is a leaf
        Leaf* leaf = reinterpret_cast<Leaf*>(node);

        if(leaf->empty()) return;

        auto flags = out.flags();

        dump_tabs();
        for(size_t i = 0; i < leaf->N; i++){
            if(i > 0) out << ", ";
            out << "<" << KEYS(leaf)[i] << ", " << VALUES(leaf)[i] << ">";
        }
        out << "\n";

        dump_tabs();
        out << "Prev: " <<  leaf->previous << ", Next: " << leaf->next;

        out << endl;

        out.setf(flags);
    }
}

void ABTree::dump_data(std::ostream& out) const {
    out << "B-Tree, intnode a: " << intnode_a << ", b: " << intnode_b << ", leaf a: " << leaf_a << ", b: " << leaf_b <<
            ", size: " << size() << std::endl;
    dump_data(out, root, 0);
}

void ABTree::dump_memory(std::ostream& out) const {
    out << "[Memory statistics] cardinality: " << size();
    auto mem_inodes = num_nodes_allocated * memsize_internal_node();
    out << ", inodes: " << num_nodes_allocated << " (" << to_string_with_unit_suffix(mem_inodes) << ")";
    auto mem_leaves = num_leaves_allocated * memsize_leaf();
    out << ", leaves: " << num_leaves_allocated << " (" << to_string_with_unit_suffix(mem_leaves) << ")";
    out << ", total: " << (num_nodes_allocated + num_leaves_allocated) << " (" << to_string_with_unit_suffix(mem_inodes + mem_leaves) << ")";
    out << std::endl;

#if defined(HAVE_LIBNUMA)
    if(numa_available() != -1){
        auto current_cpu = sched_getcpu();
        auto current_node = numa_node_of_cpu(current_cpu);
        auto tot_nodes = numa_num_configured_nodes();

        out << "--> Executing on cpu: " << current_cpu << ", node: " << current_node << ", total nodes: " << tot_nodes << "\n";

        if(tot_nodes > 1){
            // map the allocations of nodes
            std::vector<int> map_inodes(tot_nodes);
            std::vector<int> map_leaves(tot_nodes);

            std::function<void(Node*, int)> explore;
            explore = [this, &map_inodes, &map_leaves, &explore](Node* node, int level){
                bool is_leaf = level == this->height;

                // dfs on the children
                if(!is_leaf){
                    auto inode = reinterpret_cast<InternalNode*>(node);
                    auto children = CHILDREN(inode);
                    for(size_t i = 0; i < inode->N; i++){
                        explore(children[i], level +1);
                    }
                }

                int numa_node {-1};
                auto rc = get_mempolicy(&numa_node, nullptr, /* ignored*/ 0, node, MPOL_F_NODE | MPOL_F_ADDR);
                if( rc != 0 ){
                    std::cerr << "[ABTree::dump_memory] get_mempolicy: " << strerror(errno) << " (" << errno << "), logical node: " << node << std::endl;
                } else if( numa_node < 0 || numa_node >= (int) map_inodes.size() ){
                    std::cerr << "[ABTree::dump_memory] get_mempolicy, invalid result: " << numa_node << ", logical node: " << node << std::endl;
                } else if(is_leaf) {
                    map_leaves[numa_node]++;
                } else {
                    map_inodes[numa_node]++;
                }
            };

            explore(root, 1);

            for(size_t i = 0; i < map_inodes.size(); i++){
                auto mem_inodes = map_inodes[i] * memsize_internal_node();
                auto mem_leaves = map_leaves[i] * memsize_leaf();
                out << "--> Node: " << i;
                out << ", inodes: " << map_inodes[i] << " (" << to_string_with_unit_suffix(mem_inodes) << ")";
                out << ", leaves: " << map_leaves[i] << " (" << to_string_with_unit_suffix(mem_leaves) << ")";
                out << ", total: " << (map_inodes[i] + map_leaves[i]) << " (" << to_string_with_unit_suffix(mem_inodes + mem_leaves) << ")";
                out << "\n";
            }

        } // tot_nodes > 1
    }
#endif
}

void ABTree::dump() const {
    dump_memory(std::cout);
    dump_data(std::cout);
}


/*****************************************************************************
 *                                                                           *
 *   Random permutation of the allocated nodes                               *
 *                                                                           *
 *****************************************************************************/

void ABTree::set_common_memsize_nodes(bool value){
    if(size() != 0){
        throw std::logic_error("Cannot invoke the method #set_common_memsize_nodes when the data structure is not empty");
    }

    if(common_memsize != value){
        common_memsize = value;
        delete_node(root, 0);
        root = create_leaf();
    }
}
void ABTree::build(){
    bool abtree_random_permutation = false;
    try { // driver::initialize() should have been already called
        ARGREF(bool, "abtree_random_permutation").get(abtree_random_permutation); // alters the value of `abtree_random_permutation'
    } catch ( configuration::ConsoleArgumentError& e ) { } // ignore
    if(abtree_random_permutation){
        permute(ARGREF(uint64_t, "seed_random_permutation") + size());
    }
}

void ABTree::permute(uint64_t random_seed){
    if(common_memsize) {
        LOG_VERBOSE("[ABTree::permute] Permuting all memory nodes...");
        permute_allocated_nodes_nodiff(random_seed);
    } else {
        LOG_VERBOSE("[ABTree::permute] Permuting inodes with inodes and leaves with leaves...");
        permute_allocated_nodes_diff(random_seed);
    }
}

/**
 * PAND variant
 */
struct ABTree::pand_state {
    uint64_t m_seed = 1; // a random seed to generate the permutations
    std::vector<ABTree::InternalNode*> m_inodes; // the internal nodes of the ABTree
    std::vector<ABTree::Leaf*> m_leaves; // the leaves part of the ABTree instance
    std::vector<ABTree::InternalNode*> m_leaf_parents; // all parents of leaves, computed by #permute_inodes()
    std::unordered_map<ABTree::Node*, size_t> m_node2index; // the index in m_inodes and m_leaves for each node in the tree
    char* m_workspace = nullptr; // temporary workspace

    // dummy ctor
    pand_state() { };

    // disable the copy ctor
    pand_state(const pand_state&) = delete;
    pand_state& operator=(const pand_state&) = delete;

    // cleanup
    ~pand_state() { delete[] m_workspace; m_workspace = nullptr; }
};

void ABTree::permute_allocated_nodes_diff(uint64_t random_seed){
    pand_state state;
    state.m_seed = random_seed;
    pand_main(state);
}

void ABTree::pand_main(ABTree::pand_state& state){
    pand_dfs_visit_abtree(state);
    pand_permute_inodes(state);
    pand_permute_leaves(state);
}

void ABTree::pand_dfs_visit_abtree(ABTree::pand_state& state){
    // start from the root of the tree
    pand_dfs_visit_abtree(state, root, 1);
}

void ABTree::pand_dfs_visit_abtree(ABTree::pand_state& state, ABTree::Node* cnode, int depth){
    assert(cnode != nullptr);

    // for this implementation of the AB-Tree, we need to keep track of the depth to know if a node is
    // a leaf or an internal node
    if(depth == height){
        // this is a leaf
        state.m_node2index[cnode] = state.m_leaves.size();
        state.m_leaves.push_back(reinterpret_cast<ABTree::Leaf*>(cnode));
    } else {
        // this is an internal node
        state.m_node2index[cnode] = state.m_inodes.size();
        auto inode = reinterpret_cast<ABTree::InternalNode*>(cnode);
        state.m_inodes.push_back(inode);

        // repeat the visit for all the children
        auto children = CHILDREN(inode);
        for(size_t i = 0; i < inode->N; i++){
            pand_dfs_visit_abtree(state, children[i], depth +1);
        }
    }
}

void ABTree::pand_permute_inodes(ABTree::pand_state& state){
    if(state.m_inodes.size() <= 1) {
        // nothing to permute, but we need to record the parent for all children
        if(state.m_inodes.size() == 1){
            state.m_leaf_parents.push_back(state.m_inodes.back());
        }

        return; // nop
    }

    // Obtain a RandomPermutation
    std::unique_ptr<RandomPermutationParallel> rp_ptr{ new RandomPermutationParallel() };
    rp_ptr->compute(state.m_inodes.size(), state.m_seed + 1729);
    assert(rp_ptr->size() == state.m_inodes.size());

    // Copy the nodes in the workspace
    const size_t memsize_inode = memsize_internal_node();
//    std::cout << "memsize_inode: " << memsize_inode << ", inodes: " << state.m_inodes.size() << std::endl;
    state.m_workspace = new char[memsize_inode * state.m_inodes.size()];
    for(size_t i = 0, sz = rp_ptr->size(); i < sz; i++){
        size_t pI = rp_ptr->get_raw_key(i);
//        std::cout << "moving " << i << " to " << pI << std::endl;
        memcpy(state.m_workspace + pI * memsize_inode, state.m_inodes[i], memsize_inode);
    }

    // move back the nodes from the workspace to the original memory locations
    for(size_t i = 0, sz = state.m_inodes.size(); i < sz; i++){
        memcpy(state.m_inodes[i], state.m_workspace + i * memsize_inode, memsize_inode);
    }
    delete[] state.m_workspace; state.m_workspace = nullptr;

    // update the pointer to the root
    root = state.m_inodes[ rp_ptr->get_raw_key(state.m_node2index[root]) ];

    // update the pointers to the children
    for(size_t i = 0, sz = state.m_inodes.size(); i < sz; i++){
        ABTree::InternalNode* inode = state.m_inodes[i];
        assert(inode->N > 0);

        ABTree::Node** children = CHILDREN(inode);

        // check whether the children of this node are leaves or internal nodes
        bool children_are_leaves = false;
        {
            ABTree::Node* child0 = children[0];
            size_t child0_index = state.m_node2index[child0];
            children_are_leaves = (child0 == state.m_leaves[child0_index]);
        }

        // if the children are leaves, just track their parent
        if(children_are_leaves){
            state.m_leaf_parents.push_back(inode);
        } else {
            // otherwise update the pointers to the children
            for(size_t i = 0; i < inode->N; i++){
               size_t index_old = state.m_node2index[ children[i] ] ;
               size_t index_perm = rp_ptr->get_raw_key(index_old);
               children[i] = state.m_inodes[index_perm];
            }
        }
    }
}

void ABTree::pand_permute_leaves(ABTree::pand_state& state){
    if(state.m_leaves.size() <= 1){ return; } // nothing to do

    // Obtain a RandomPermutation
    std::unique_ptr<RandomPermutationParallel> rp_ptr{ new RandomPermutationParallel() };
    rp_ptr->compute(state.m_leaves.size(), state.m_seed + 1731);
    assert(rp_ptr->size() == state.m_leaves.size());

    // permute the nodes in the workspace
    const size_t memsize_leaves = memsize_leaf();
//    std::cout << "memsize_leaves: " << memsize_leaves << ", leaves: " << state.m_leaves.size() << std::endl;
    state.m_workspace = new char[memsize_leaves * state.m_leaves.size()];
    for(size_t i = 0, sz = rp_ptr->size(); i < sz; i++){
        ABTree::Leaf* leaf = state.m_leaves[i];
        size_t pI = rp_ptr->get_raw_key(i);
//        std::cout << "moving " << i << " to " << pI << std::endl;

        // update the prev/next pointer
        if(leaf->previous) {
            size_t iP = state.m_node2index[leaf->previous];
            assert(iP < state.m_leaves.size());
            leaf->previous = state.m_leaves[ rp_ptr->get_raw_key(iP) ];
        }
        if(leaf->next) {
            size_t iN = state.m_node2index[leaf->next];
            assert(iN < state.m_leaves.size());
            leaf->next = state.m_leaves[ rp_ptr->get_raw_key(iN) ];
        }

        memcpy(state.m_workspace + pI * memsize_leaves, leaf, memsize_leaves);
    }

    // move back the leaves from the workspace to the original memory locations
    for(size_t i = 0, sz = state.m_leaves.size(); i < sz; i++){
        memcpy(state.m_leaves[i], state.m_workspace + i * memsize_leaves, memsize_leaves);
    }
    delete[] state.m_workspace; state.m_workspace = nullptr;

    // update the pointers from the parents
    for(auto parent: state.m_leaf_parents){
        auto children = CHILDREN(parent);
        for(size_t i = 0, N = parent->N; i < N; i++){
            children[i] = state.m_leaves[ rp_ptr->get_raw_key( state.m_node2index[children[i]] ) ];
        }
    }
}

/**
 * PANN variant
 */

// Time the single operations ?
//#define TIMER_PANN

#if defined(TIMER_PANN)
namespace {
struct TIMER_PANN_INFO{
    const std::string header;
    Timer timer;

    TIMER_PANN_INFO(const std::string& header) : header(header), timer(true) { }

    ~TIMER_PANN_INFO(){
        std::cout << "[ABTree::pann_main] [Timer] " << header << ". Elapsed time: " << timer.milliseconds() << " milliseconds." << std::endl;
    }
};
}
#define TIMER_PANN_VARIABLE(timer) _local_##timer
#define TIMER_PANN_START(name) TIMER_PANN_INFO TIMER_PANN_VARIABLE(name){ #name }
#define TIMER_PANN_STOP(name) TIMER_PANN_VARIABLE(name).timer.stop()
#else
#define TIMER_PANN_START(name)
#define TIMER_PANN_STOP(name)
#endif

struct ABTree::pann_state {
    struct NodeDescription{
        ABTree::Node* pointer;
        bool is_leaf;
    };

    uint64_t m_seed = 1; // a random seed to generate the permutations
    std::vector<NodeDescription> m_nodes; // the list of all nodes in the B-Tree
    std::unordered_map<ABTree::Node*, size_t> m_node2index; // the index in m_inodes and m_leaves for each node in the tree
    char* m_workspace = nullptr; // temporary workspace

    // dummy ctor
    pann_state() { };

    // disable the copy ctor
    pann_state(const pand_state&) = delete;
    pann_state& operator=(const pand_state&) = delete;

    // cleanup
    ~pann_state() { delete[] m_workspace; m_workspace = nullptr; }
};

void ABTree::permute_allocated_nodes_nodiff(uint64_t random_seed){
    pann_state state;
    state.m_seed = random_seed;
    pann_main(state);
}

void ABTree::pann_main(ABTree::pann_state& state){
    // Edge case
    if(height == 1) return;

    // Init the state
    TIMER_PANN_START(pann_dfs_visit);
    pann_dfs_visit(state, root, 1);
    TIMER_PANN_STOP(pann_dfs_visit);

    // Obtain a permutation of the random nodes
    TIMER_PANN_START(random_permutation);
    std::unique_ptr<RandomPermutationParallel> rp_ptr( new RandomPermutationParallel );
    RandomPermutationParallel* rp = rp_ptr.get();
    rp->compute(state.m_nodes.size(), state.m_seed + 1787);
    TIMER_PANN_STOP(random_permutation);

    // Permute the nodes
    assert(memsize_internal_node() == memsize_leaf());
    size_t memsize_node = memsize_internal_node();
    state.m_workspace = new char[memsize_node * state.m_nodes.size()];
    TIMER_PANN_START(permute_nodes);
    for(size_t i = 0, sz = state.m_nodes.size(); i < sz; i++){
        size_t pI = rp->get_raw_key(i);
//        std::cout << "moving " << i << " to " << pI << std::endl;

        // update the pointers to the children
        if(!state.m_nodes[i].is_leaf){
            InternalNode* inode = reinterpret_cast<InternalNode*>(state.m_nodes[i].pointer);
            auto children = CHILDREN(inode);
            for(size_t j = 0; j < inode->N; j++){
                children[j] = state.m_nodes[ rp->get_raw_key( state.m_node2index[children[j]] ) ].pointer;
            }
        } else {
            // update the previous/next pointer
            Leaf* leaf = reinterpret_cast<Leaf*>(state.m_nodes[i].pointer);
            if(leaf->previous){
                leaf->previous = (Leaf*) state.m_nodes[ rp->get_raw_key( state.m_node2index[leaf->previous]) ].pointer;
            }
            if(leaf->next){
                leaf->next = (Leaf*) state.m_nodes[ rp->get_raw_key( state.m_node2index[leaf->next]) ].pointer;
            }

        }

        memcpy(state.m_workspace + pI * memsize_node, state.m_nodes[i].pointer, memsize_node);
    }
    TIMER_PANN_STOP(permute_nodes);

    // Move the nodes to the memory allocations performed by the AB-Tree
    TIMER_PANN_START(copy_workspace);
    for(size_t i = 0, sz = state.m_nodes.size(); i < sz; i++){
        memcpy(state.m_nodes[i].pointer, state.m_workspace + i * memsize_node, memsize_node);
    }
    TIMER_PANN_STOP(copy_workspace);

    delete[] state.m_workspace; state.m_workspace = nullptr;

    // Update the pointer to the root
    root = state.m_nodes[ rp->get_raw_key(state.m_node2index[root]) ].pointer;
}

void ABTree::pann_dfs_visit(ABTree::pann_state& state, ABTree::Node* node, int depth){
    assert(node != nullptr);

    bool is_leaf = (depth == height);

    state.m_node2index[node] = state.m_nodes.size();
    state.m_nodes.push_back( { node, is_leaf } );

    if(!is_leaf){ // visit the children
        auto inode = reinterpret_cast<ABTree::InternalNode*>(node);
        auto children = CHILDREN(inode);
        for(size_t i = 0; i < inode->N; i++){
            pann_dfs_visit(state, children[i], depth +1);
        }
    }
}

} // namespace pma
