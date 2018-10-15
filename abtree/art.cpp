/*
 * art_tree.cpp
 *
 *  Created on: 24 Apr 2018
 *      Author: Dean De Leo
 *
 * Wrapper for the Adaptive Radix Tree implementation by Florian Scheibner
 * https://github.com/flode/ARTSynchronized
 */

#include "art.hpp"

#include <cassert>
#include <cstdlib> // free, posix_memalign
#include <iomanip>
#include <iostream>

#include "miscellaneous.hpp"

using namespace std;

namespace abtree {

/*****************************************************************************
 *                                                                           *
 *   DEBUG                                                                   *
 *                                                                           *
 *****************************************************************************/
//#define DEBUG
#if defined(DEBUG)
    #define COUT_DEBUG(msg) std::cout << "[ART::" << __FUNCTION__ << "] " << msg << std::endl
#else
    #define COUT_DEBUG(msg)
#endif


/*****************************************************************************
 *                                                                           *
 *   Initialisation                                                          *
 *                                                                           *
 *****************************************************************************/

ART::ART(uint64_t leaf_block_size) :
                m_load_key{new LoadKeyImpl(this)},
                m_index(m_load_key),
                m_first(nullptr),
                m_leaf_block_size(leaf_block_size),
                m_cardinality(0),
                m_leaf_count(0){
    m_first = create_leaf();
}

ART::~ART(){

    // delete the leaves
    Leaf* leaf = m_first;
    while(leaf != nullptr){
        Leaf* pointer = leaf;
        leaf = leaf->next; // next element

        free(pointer);
    }
    m_leaf_count = 0;
}

/*****************************************************************************
 *                                                                           *
 *   Miscellaneous                                                           *
 *                                                                           *
 *****************************************************************************/

size_t ART::size() const {
    return m_cardinality;
}

bool ART::empty() const noexcept {
    return m_cardinality == 0;
}

size_t ART::memsize_leaf() const {
    return sizeof(Leaf) + sizeof(int64_t) * 2 /* key/value */ * m_leaf_block_size;
}

ART::Leaf* ART::create_leaf() {
    static_assert(!is_polymorphic<Leaf>::value, "Expected a non polymorphic type (no vtable)");
    static_assert(sizeof(Leaf) == 24, "Expected 24 bytes for the cardinality + ptr previous + ptr next");

    // (cardinality) 1 + (ptr left/right) 2 + (keys=) leaf_b + (values) leaf_b == 2 * leaf_b + 1;
    Leaf* ptr (nullptr);
    int rc = posix_memalign((void**) &ptr, /* alignment = */ 64,  /* size = */ memsize_leaf());
    if(rc != 0) throw std::runtime_error("ART_nr::create_leaf, cannot obtain a chunk of aligned memory");
    ptr->N = 0;
    ptr->next = ptr->previous = nullptr;

    m_leaf_count++;
    return ptr;
}

int64_t ART::get_pivot(void* pointer) const {
    Leaf* leaf = reinterpret_cast<Leaf*>(pointer);
    assert(leaf->N > 0 && "This leaf is empty!");
    return KEYS(leaf)[0];
}

int64_t* ART::KEYS(const Leaf* leaf) const {
    Leaf* instance = const_cast<Leaf*>(leaf);
    return reinterpret_cast<int64_t*>(reinterpret_cast<uint8_t*>(instance) + sizeof(Leaf));
}

int64_t* ART::VALUES(const Leaf* leaf) const {
    return KEYS(leaf) + m_leaf_block_size;
}

size_t ART::memory_footprint() const {
    size_t space_index = m_index.memory_footprint();
    size_t space_leaves = m_leaf_count * memsize_leaf();

    return space_index + space_leaves;
}

/*****************************************************************************
 *                                                                           *
 *   LoadKeyImpl                                                             *
 *                                                                           *
 *****************************************************************************/

ART::LoadKeyImpl::LoadKeyImpl(ART* art) : m_art(art){ } ;

void ART::LoadKeyImpl::operator() (const ART_unsynchronized::TID tid, ART_unsynchronized::Key& key) {
    // tid is the address of the leaf, key is the minimum key in the leaf
    int64_t pivot = m_art->get_pivot(reinterpret_cast<void*>(tid));
    store(pivot, key);
}

void ART::LoadKeyImpl::store(int64_t pivot, ART_unsynchronized::Key& key){
    key.setKeyLen(sizeof(pivot));
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(pivot);
}

/*****************************************************************************
 *                                                                           *
 *   Insert                                                                  *
 *                                                                           *
 *****************************************************************************/
void ART::insert(int64_t key, int64_t value) {
    COUT_DEBUG("key: " << key << ", value: " << value);

    if (UNLIKELY( empty() )){
        leaf_insert(m_first, key, value);
        index_insert(key, m_first);
    } else {
        // find the leaf where to insert the element
        Leaf* leaf = index_find_leq(key);
        if(leaf == nullptr){ leaf = m_first; }

        // split the leaf node if it's full
        if(leaf->N == m_leaf_block_size){
            auto pair = split(leaf);
            index_insert(pair);

            // if the key is greater than the pivot of the right leaf,
            // then insert on it, rather than using the left leaf.
            if(key >= pair.first){
                leaf = pair.second;
            }
        }

        // insert the element in the leaf
        assert(leaf->N > 0 && "Empty leaf!");
        int64_t pivot_old = KEYS(leaf)[0];
        int64_t pivot_new = leaf_insert(leaf, key, value);

        // update the radix tree
        if(pivot_old != pivot_new){
            index_remove(pivot_old, leaf);
            index_insert(pivot_new, leaf);
        }
    }

#if defined(DEBUG)
    dump();
#endif
}

int64_t ART::leaf_insert(Leaf* leaf, int64_t key, int64_t value){
    COUT_DEBUG("leaf: " << leaf << ", key: " << key << ", value: " << value);
    assert(leaf->N < m_leaf_block_size);
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
    m_cardinality += 1;

    return keys[0];
}

void ART::index_insert(int64_t key, Leaf* value){
    assert(value != nullptr && "Invalid value");
    COUT_DEBUG("key: " << key << ", value: " << value);

    ART_unsynchronized::Key art_key;
    m_load_key->store(key, art_key);
    ART_unsynchronized::TID art_value = reinterpret_cast<ART_unsynchronized::TID>(value);
    m_index.insert(art_key, art_value);
}

void ART::index_insert(pair<int64_t, Leaf*> pair){
    index_insert(pair.first, pair.second);
}


/*****************************************************************************
 *                                                                           *
 *   Split                                                                   *
 *                                                                           *
 *****************************************************************************/
pair<int64_t, ART::Leaf*> ART::split(Leaf* l1){
    assert(l1->N <= m_leaf_block_size && "Invalid cardinality");
    assert(l1->N == m_leaf_block_size && "The capacity of this leaf is not filled, no reason to split!");
    COUT_DEBUG("split: " << l1);

    Leaf* l2 = create_leaf();

    size_t thres = (l1->N +1) /2;
    l2->N = l1->N - thres;
    l1->N = thres;

    // move the elements from l1 to l2
    memcpy(KEYS(l2), KEYS(l1) + thres, l2->N * sizeof(KEYS(l2)[0]));
    memcpy(VALUES(l2), VALUES(l1) + thres, l2->N * sizeof(VALUES(l2)[0]));

    // adjust the links
    l2->next = l1->next;
    if( l2->next != nullptr ) { l2->next->previous = l2; }
    l2->previous = l1;
    l1->next = l2;

    int64_t pivot2 = KEYS(l2)[0];

    return {pivot2, l2};
}


/*****************************************************************************
 *                                                                           *
 *   Remove                                                                  *
 *                                                                           *
 *****************************************************************************/
int64_t ART::remove(int64_t key) {
    if(UNLIKELY(empty())) return -1;
    Leaf* leaf = index_find_leq(key);
    if(leaf == nullptr) return -1;

    bool update_min;
    int64_t value = leaf_remove(key, leaf, &update_min);

    if(update_min){
        assert(leaf != nullptr && "#leaf_remove set leaf to nullptr while setting leaf == nullptr");
        index_remove(key, leaf);
        assert(m_cardinality == 0 || leaf->N > 0);
        if(leaf->N > 0){ index_insert( KEYS(leaf)[0], leaf); };
    }

    if(value >= 0){
        rebalance(leaf);
    }

#if defined(DEBUG)
    dump();
#endif

    return value;
}

int64_t ART::leaf_remove(int64_t key, Leaf*& leaf, bool* out_update_min){
    assert(leaf != nullptr && leaf->N > 0 && "Empty leaf");
    COUT_DEBUG("key: " << key << ", leaf: " << leaf);
    bool stop = false;
    bool found = false;
    bool update_min = false;
    int64_t value = -1;

    do {
        int64_t N = leaf->N;
        int64_t* __restrict keys = KEYS(leaf);
        int64_t* __restrict values = VALUES(leaf);

        if(keys[0] > key){
            stop = true;
        } else if(keys[N-1] < key){
            leaf = leaf->next;
        } else {
            int64_t i = 0;
            while(i < N && keys[i] < key) i++;
            if(i < N && keys[i] == key){
                value = values[i];
                for(size_t j = i; j < leaf->N -1; j++){
                    keys[j] = keys[j+1];
                    values[j] = values[j+1];
                }
                leaf->N -= 1;
                found = true;
                update_min = (i == 0);
            }
            stop = true;
        }
    } while(leaf != nullptr && !stop);


    if(found){
        m_cardinality--;
    } else {
        leaf = nullptr;
    }

    if(out_update_min != nullptr){ *out_update_min = update_min; }
    return value;
}


void ART::index_remove(int64_t key, Leaf* value){
    COUT_DEBUG("key: " << key << ", value: " << value);
    assert(value != nullptr && "Invalid value");

    ART_unsynchronized::Key art_key;
    m_load_key->store(key, art_key);
    ART_unsynchronized::TID art_value = reinterpret_cast<ART_unsynchronized::TID>(value);
    m_index.remove(art_key, art_value);
}


/*****************************************************************************
 *                                                                           *
 *   Rebalance                                                               *
 *                                                                           *
 *****************************************************************************/
void ART::rebalance(Leaf* leaf){
    if(leaf == nullptr || m_leaf_count <= 1 || leaf->N >= m_leaf_block_size/2) return;
    const int64_t lowerbound = m_leaf_block_size/2;
    int64_t need = lowerbound - leaf->N;

    // check if we can steal `need' nodes from its sibling
    if(leaf->previous != nullptr && (leaf->next == nullptr || leaf->previous->N >= leaf->next->N) && leaf->previous->N >= lowerbound + need){ // steal from left
        share_left(leaf, need);
    } else if(leaf->next != nullptr && leaf->next->N >= lowerbound + need){ // steal from right
        share_right(leaf, need);
    }
    // merge with one of its siblings
    else if( leaf->previous != nullptr ) {
        merge(leaf->previous, leaf);
    } else {
        assert(leaf->next != nullptr && "Otherwise m_leaf_count == 1");
        merge(leaf, leaf->next);
    }
}

void ART::share_left(Leaf* leaf, int64_t need){
    assert(leaf != nullptr && "Null pointer (leaf)");
    assert(leaf->previous != nullptr && "Null pointer (left link)");
    assert(leaf->previous->N >= m_leaf_block_size/2 + need && "The left sibling doesn't contain `need' elements to share");

    Leaf* l1 = leaf->previous;
    Leaf* l2 = leaf;

    int64_t* __restrict l1_keys = KEYS(l1);
    int64_t* __restrict l1_values = VALUES(l1);
    int64_t* __restrict l2_keys = KEYS(l2);
    int64_t* __restrict l2_values = VALUES(l2);

    // remove the current separator key from the index
    index_remove(l2_keys[0], l2);

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

    // re-insert the new separator key in the index
    index_insert(l2_keys[0], l2);

    // update the size of the elements
    l2->N += need;
    l1->N -= need;
}

void ART::share_right(Leaf* leaf, int64_t need){
    assert(leaf != nullptr && "Null pointer (leaf)");
    assert(leaf->next != nullptr && "Null pointer (right link)");
    assert(leaf->next->N >= m_leaf_block_size/2 + need && "The left sibling doesn't contain `need' elements to share");

    Leaf* l1 = leaf;
    Leaf* l2 = leaf->next;

    int64_t* __restrict l1_keys = KEYS(l1);
    int64_t* __restrict l1_values = VALUES(l1);
    int64_t* __restrict l2_keys = KEYS(l2);
    int64_t* __restrict l2_values = VALUES(l2);

    // remove the current separator key from the index
    index_remove(l2_keys[0], l2);

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

    // re-insert the new separator key in the index
    index_insert(l2_keys[0], l2);

    // update the cardinalities of the nodes
    l1->N += need;
    l2->N -= need;
}

void ART::merge(Leaf* l1, Leaf* l2){
    assert(l1 != nullptr && "Null pointer (l1)");
    assert(l2 != nullptr && "Null pointer (l2)");
    assert(l1->N + l2->N <= m_leaf_block_size);

    // move all elements from l2 to l1
    memcpy(KEYS(l1) + l1->N, KEYS(l2), l2->N * sizeof(KEYS(l2)[0]));
    memcpy(VALUES(l1) + l1->N, VALUES(l2), l2->N * sizeof(VALUES(l2)[0]));

    // update the cardinalities of the two leaves
    l1->N += l2->N;
    l2->N = 0;

    // adjust the links
    l1->next = l2->next;
    if(l2->next != nullptr){ l2->next->previous = l1; }

    // update the index
    index_remove(KEYS(l2)[0], l2);

    // free the memory from l2
    free(l2); l2 = nullptr;
    m_leaf_count--;
}


/*****************************************************************************
 *                                                                           *
 *   Lookup                                                                  *
 *                                                                           *
 *****************************************************************************/

ART::Leaf* ART::index_find_leq(int64_t key) const {
    ART_unsynchronized::Key art_key;
    m_load_key->store(key, art_key);

    ART_unsynchronized::TID art_value = m_index.findLessOrEqual(art_key);
    return reinterpret_cast<Leaf*>(art_value);
}

int64_t ART::leaf_find(Leaf* leaf, int64_t key) const noexcept {
    COUT_DEBUG("leaf: " << leaf << ", key: " << key);
    size_t i = 0, N = leaf->N;
    int64_t* __restrict keys = KEYS(leaf);
    while(i < N && keys[i] < key) i++;

    return (i < N && keys[i] == key) ? i : -1;
}

int64_t ART::find(int64_t key) const {
    Leaf* leaf = index_find_leq(key);
    COUT_DEBUG("Lookup: " << key << ", leaf: " << leaf);
    if(leaf == nullptr) return -1;
    int64_t index = leaf_find(leaf, key);
    COUT_DEBUG("key: " << key << ", leaf: " << leaf << ", index: " << index);
    if(index < 0) return -1;
    return VALUES(leaf)[index];
}

/*****************************************************************************
 *                                                                           *
 *   Iterator                                                                *
 *                                                                           *
 *****************************************************************************/
ART::Iterator::Iterator(const ART* tree, int64_t max, Leaf* block, int64_t pos):
        tree(tree), max(max), block(block), pos(pos) {
//    COUT_DEBUG("column_no: " << column_no << ", max: " << max << ", block: " << block << ", pos: " << pos);
}


bool ART::Iterator::hasNext() const { return block != nullptr; }

std::pair<int64_t, int64_t> ART::Iterator::next() {
    if(!block) return pair<int64_t, int64_t>{-1, -1};

    auto v = make_pair(tree->KEYS(block)[pos], tree->VALUES(block)[pos]);

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

unique_ptr<ART::Iterator> ART::create_iterator(int64_t max, Leaf* leaf, int64_t pos) const {
    if(leaf == nullptr || KEYS(leaf)[pos] > max){
        return unique_ptr<Iterator>(new ART::Iterator(this, max, nullptr, 0));
    } else {
        return unique_ptr<Iterator>(new ART::Iterator(this, max, leaf, pos));
    }
}

unique_ptr<ART::Iterator> ART::leaf_iterator(Leaf* leaf, int64_t min, int64_t max) const {
    if(leaf == nullptr) leaf = m_first;
    if(leaf->N == 0){ return create_iterator(max, nullptr, 0); } // empty tree

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

unique_ptr<pma::Iterator> ART::find(int64_t min, int64_t max) const {
    if(min > max) return create_iterator(max, nullptr, 0);

    Leaf* starting_point = index_find_leq(min);
    return leaf_iterator(starting_point, min, max);
}

unique_ptr<pma::Iterator> ART::iterator() const {
    return leaf_iterator(m_first, numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());
}

/*****************************************************************************
 *                                                                           *
 *   Range queries                                                           *
 *                                                                           *
 *****************************************************************************/

pma::Interface::SumResult ART::leaf_sum(Leaf* leaf, int64_t i, int64_t max) const {
    assert(leaf != nullptr && "Null pointer");
    COUT_DEBUG("leaf: " << leaf << ", i: " << i << ", max: " << max);

    // standard case, find the first key that satisfies the interval
    int64_t* __restrict keys = KEYS(leaf);
    int64_t* __restrict values = VALUES(leaf);
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

pma::Interface::SumResult ART::sum(int64_t min, int64_t max) const {
    using SumResult = pma::Interface::SumResult;
    if(min > max) return SumResult{};
    if(min < 0) min = 0; // this is because ART uses the most significant bit internally as a marker

    Leaf* leaf = index_find_leq(min);
    if(leaf == nullptr) leaf = m_first;
    if(leaf->N == 0){ return SumResult{}; } // empty tree

    // edge case, the interval starts at the sibling leaf
    if(KEYS(leaf)[leaf->N -1] < min){
        leaf = leaf->next;
        if(leaf == nullptr) {
            return SumResult{};
        } else if (KEYS(leaf)[0] >= min){
            return leaf_sum(leaf, 0, max);
        } else {
            return SumResult{};
        }
    }

    // edge case, the interval should have started before this leaf
    if (KEYS(leaf)[0] > max) return SumResult{};

    // standard case, find the first key that satisfies the interval

    size_t i = 0;
    while(i < leaf->N && KEYS(leaf)[i] < min) i++;
    return leaf_sum(leaf, i, max);
}

/******************************************************************************
 *                                                                            *
 *   Dump                                                                     *
 *                                                                            *
 *****************************************************************************/

static void dump_tabs(int depth){
    auto flags = cout.flags();
    cout << setw(depth * 2) << setfill(' ') << ' ';
    cout.setf(flags);
};

void ART::dump_leaves() const {
    size_t i = 0;
    Leaf* leaf = m_first;
#if !defined(NDEBUG)
    int64_t key_previous = numeric_limits<int64_t>::min();
#endif

    while(leaf != nullptr){
        cout << "[Leaf #" << i << "] " << leaf << ", N: " << leaf->N;
        cout << ", Prev: " <<  leaf->previous << ", Next: " << leaf->next << "\n";

        dump_tabs(2);
        for(size_t i = 0; i < leaf->N; i++){
            if(i > 0) cout << ", ";
            cout << "<" << KEYS(leaf)[i] << ", " << VALUES(leaf)[i] << ">";

#if !defined(NDEBUG)
            assert(key_previous <= KEYS(leaf)[i] && "Sorted order not respected");
            key_previous = KEYS(leaf)[i];
#endif
        }
        cout << "\n";

        // move to the next leaf
        leaf = leaf->next;
        i++;
    }
}

void ART::dump() const {
    cout << "ART layout, leaf_block_size: " << m_leaf_block_size << ", cardinality: " << size() << endl;

    // Dump the index
    cout << "ART index: " << endl;
    m_index.dump();

    // Dump the leaves
    cout << "Leaves: " << endl;
    dump_leaves();
}

} // namespace abtree
