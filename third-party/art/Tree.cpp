#include <assert.h>
#include <algorithm>
#include <iostream>
#include "Tree.h"
//#include "../Epoche.cpp"

//#define DEBUG
#define COUT_DEBUG_FORCE(msg) std::cout << "[" << __FILE__ << ":" << __FUNCTION__ << ":" << __LINE__ << "] " << msg << std::endl
#if defined(DEBUG)
    #define COUT_DEBUG(msg) COUT_DEBUG_FORCE(msg)
#else
    #define COUT_DEBUG(msg)
#endif

// hack, these are global variables. To keep track of the memory footprint
static uint64_t _private_num_nodes_N4 = 0;
static uint64_t _private_num_nodes_N16 = 0;
static uint64_t _private_num_nodes_N48 = 0;
static uint64_t _private_num_nodes_N256 = 0;

#include "N_impl.hpp"

using namespace std;

namespace ART_unsynchronized {

    Tree::Tree(shared_ptr<LoadKeyInterface> loadKey) : root(new N256(nullptr, 0)), loadKey(move(loadKey)) {
    }

    Tree::~Tree() {
        N::deleteChildren(root);
        N::deleteNode(root);
    }

    TID Tree::lookup(const Key &k) const {
        N *node = nullptr;
        N *nextNode = root;
        uint32_t level = 0;
        bool optimisticPrefixMatch = false;

        while (true) {
            node = nextNode;
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    return 0;
                case CheckPrefixResult::OptimisticMatch:
                    optimisticPrefixMatch = true;
                    // fallthrough
                case CheckPrefixResult::Match:
                    if (k.getKeyLen() <= level) {
                        return 0;
                    }
                    nextNode = N::getChild(k[level], node);

                    if (nextNode == nullptr) {
                        return 0;
                    }
                    if (N::isLeaf(nextNode)) {
                        TID tid = N::getLeaf(nextNode);
                        if (level < k.getKeyLen() - 1 || optimisticPrefixMatch) {
                            return checkKey(tid, k);
                        }
                        return tid;
                    }
                    level++;
            }
        }
    }

    bool Tree::findLessOrEqual(const Key& key, N* node, uint32_t level, TID* output_result) const {
        assert(node != nullptr);
        assert(output_result != nullptr);

        // base case, the current node is a leaf
        if(N::isLeaf(node)){
            TID leaf_value = N::getLeaf(node);
            *output_result = leaf_value;
            Key leaf_key;
            (*loadKey)(leaf_value, leaf_key);
            while(level < leaf_key.getKeyLen() && level < key.getKeyLen()){
                if(key[level] < leaf_key[level]){
                    return false;
                } else if(key[level] == leaf_key[level]) {
                    level++;
                } else { // key[level] > leaf_key[level]
                    return true;
                }
            }

            if(key.getKeyLen() == leaf_key.getKeyLen()){ // same value
                return true;
            } else if (key.getKeyLen() <= level){ // key < leaf_key
                return false;
            } else { // leaf_key.getKeyLen() <= level --> key > leaf_key
                return true;
            }
        }

        // first check the damn prefix
        auto prefixResult = checkPrefixCompare(node, key, level, loadKey.get());
        switch(prefixResult){
        case PCCompareResults::Smaller: {
            // counterintuitively, it means that the prefix of the node is lesser than the key
            // i.e. the key is bigger than any element in this node
            *output_result = N::getMaxLeaf(node);
            return true;
            // the correct child is the maximum of the previous sibling. Ask the parent to fetch it.
            return false;
        } break;
        case PCCompareResults::Equal: {
            /* nop */
        } break;
        case PCCompareResults::Bigger: {
            // counterintuitively, it means that the prefix of the node is greater than the key
            // ask the parent to return the max for the sibling that precedes this node
            return false;
        } break;
        } // end switch

        // second, find the next node to percolate in the tree
        bool exact_match; // set by N::getChildLessOrEqual as side effect
        auto child = N::getChildLessOrEqual(node, key[level], &exact_match);
        COUT_DEBUG("node: " << node << ", level: " << level << ", child: " << child << ", exact_match: " << exact_match);
        if(child == nullptr){
            return false; // again, ask the parent to return the maximum of the previous sibling
        } else if (exact_match){ // percolate the tree
            bool match = findLessOrEqual(key, child, level +1, output_result);

            if(match){ // found ?
                return true;
            } else {
                // then the correct is the maximum of the previous sibling
                auto sibling = N::getPredecessor(node, key[level]);
                COUT_DEBUG("node: " << node << ", sibling: " << sibling);
                if(sibling != nullptr){
                    *output_result = N::getMaxLeaf(sibling);
                    return true;
                } else {
                    // ask the parent
                    return false;
                }
            }
        } else { // key[level] > child[level], but it is lower than all other children => return the max from the given child
            *output_result = N::getMaxLeaf(child);
            return true;
        }
    }

    TID Tree::findLessOrEqual(const Key& key) const {
        TID result;
        auto match = findLessOrEqual(key, root, 0, &result);
        if(match){
            return result;
        } else {
            return 0;
        }
    }

    TID Tree::checkKey(const TID tid, const Key &k) const {
        Key kt;
        (*loadKey)(tid, kt);
        if (k == kt) {
            return tid;
        }
        return 0;
    }

    void Tree::insert(const Key &k, TID tid) {
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;

            uint32_t nextLevel = level;

            uint8_t nonMatchingKey;
            Prefix remainingPrefix;
            switch (checkPrefixPessimistic(node, k, nextLevel, nonMatchingKey, remainingPrefix,
                                                           loadKey.get())) { // increases level
                case CheckPrefixPessimisticResult::NoMatch: {
                    assert(nextLevel < k.getKeyLen()); //prevent duplicate key
                    // 1) Create new node which will be parent of node, Set common prefix, level to this node
                    auto newNode = new N4(node->getPrefix(), nextLevel - level);

                    // 2)  add node and (tid, *k) as children
                    newNode->insert(k[nextLevel], N::setLeaf(tid));
                    newNode->insert(nonMatchingKey, node);

                    // 3) update parentNode to point to the new node
                    N::change(parentNode, parentKey, newNode);

                    // 4) update prefix of node
                    node->setPrefix(remainingPrefix,
                                    node->getPrefixLength() - ((nextLevel - level) + 1));

                    return;
                }
                case CheckPrefixPessimisticResult::Match:
                    break;
            }
            assert(nextLevel < k.getKeyLen()); //prevent duplicate key
            level = nextLevel;
            nodeKey = k[level];
            nextNode = N::getChild(nodeKey, node);

            if (nextNode == nullptr) {
                N::insertA(node, parentNode, parentKey, nodeKey, N::setLeaf(tid));
                return;
            }
            if (N::isLeaf(nextNode)) {
                Key key;
                (*loadKey)(N::getLeaf(nextNode), key);

                level++;
                assert(level < key.getKeyLen()); //prevent inserting when prefix of key exists already
                uint32_t prefixLength = 0;
                while (key[level + prefixLength] == k[level + prefixLength]) {
                    prefixLength++;
                }

                auto n4 = new N4(&k[level], prefixLength);
                n4->insert(k[level + prefixLength], N::setLeaf(tid));
                n4->insert(key[level + prefixLength], nextNode);
                N::change(node, k[level - 1], n4);
                return;
            }

            level++;
        }
    }

    void Tree::remove(const Key &k, TID tid) {
        N *node = nullptr;
        N *nextNode = root;
        N *parentNode = nullptr;
        uint8_t parentKey, nodeKey = 0;
        uint32_t level = 0;
        //bool optimisticPrefixMatch = false;

        while (true) {
            parentNode = node;
            parentKey = nodeKey;
            node = nextNode;

//            COUT_DEBUG("checkPrefix node: " << node << ", k: " << k << ", level: " << level << ", output: " << (p == CheckPrefixResult::NoMatch ? "NoMatch" : p == CheckPrefixResult::OptimisticMatch ? "OptimisticMatch" : "Match"));
            switch (checkPrefix(node, k, level)) { // increases level
                case CheckPrefixResult::NoMatch:
                    return;
                case CheckPrefixResult::OptimisticMatch:
                    // fallthrough
                case CheckPrefixResult::Match: {
                    nodeKey = k[level];
                    nextNode = N::getChild(nodeKey, node);

                    if (nextNode == nullptr) {
                        return;
                    }
                    if (N::isLeaf(nextNode)) {
                        if (N::getLeaf(nextNode) != tid) {
                            return;
                        }
                        assert(parentNode == nullptr || node->getCount() != 1);
                        if (node->getCount() == 2 && node != root) {
                            // 1. check remaining entries
                            N *secondNodeN;
                            uint8_t secondNodeK;
                            std::tie(secondNodeN, secondNodeK) = N::getSecondChild(node, nodeKey);
                            if (N::isLeaf(secondNodeN)) {

                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);

                                delete node;
                            } else {
                                //N::remove(node, k[level]); not necessary
                                N::change(parentNode, parentKey, secondNodeN);
                                secondNodeN->addPrefixBefore(node, secondNodeK);

                                delete node;
                            }
                        } else {
                            N::removeA(node, k[level], parentNode, parentKey);
                        }
                        return;
                    }
                    level++;
                }
            }
        }
    }


    inline typename Tree::CheckPrefixResult Tree::checkPrefix(N *n, const Key &k, uint32_t &level) {
        if (k.getKeyLen() <= level + n->getPrefixLength()) {
            return CheckPrefixResult::NoMatch;
        }
        if (n->hasPrefix()) {
            for (uint32_t i = 0; i < std::min(n->getPrefixLength(), maxStoredPrefixLength); ++i) {
                if (n->getPrefix()[i] != k[level]) {
                    return CheckPrefixResult::NoMatch;
                }
                ++level;
            }
            if (n->getPrefixLength() > maxStoredPrefixLength) {
                level += n->getPrefixLength() - maxStoredPrefixLength;
                return CheckPrefixResult::OptimisticMatch;
            }
        }
        return CheckPrefixResult::Match;
    }

    typename Tree::CheckPrefixPessimisticResult Tree::checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                        uint8_t &nonMatchingKey,
                                                                        Prefix &nonMatchingPrefix,
                                                                        LoadKeyInterface* loadKey) {
        if (n->hasPrefix()) {
            uint32_t prevLevel = level;
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    (*loadKey)(N::getAnyChildTid(n), kt);
                }
                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey != k[level]) {
                    nonMatchingKey = curKey;
                    if (n->getPrefixLength() > maxStoredPrefixLength) {
                        if (i < maxStoredPrefixLength) {
                            (*loadKey)(N::getAnyChildTid(n), kt);
                        }
                        for (uint32_t j = 0; j < std::min((n->getPrefixLength() - (level - prevLevel) - 1),
                                                          maxStoredPrefixLength); ++j) {
                            nonMatchingPrefix[j] = kt[level + j + 1];
                        }
                    } else {
                        for (uint32_t j = 0; j < n->getPrefixLength() - i - 1; ++j) {
                            nonMatchingPrefix[j] = n->getPrefix()[i + j + 1];
                        }
                    }
                    return CheckPrefixPessimisticResult::NoMatch;
                }
                ++level;
            }
        }
        return CheckPrefixPessimisticResult::Match;
    }

    typename Tree::PCCompareResults Tree::checkPrefixCompare(N *n, const Key &k, uint32_t &level,
                                                                        LoadKeyInterface* loadKey) {
        if (n->hasPrefix()) {
            COUT_DEBUG("Key: " << k << ", level: " << level);
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    (*loadKey)(N::getAnyChildTid(n), kt);
                }
                uint8_t kLevel = (k.getKeyLen() > level) ? k[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                COUT_DEBUG("i: " << i << ", level: " << level << ", kLevel: " << (int) kLevel << ", curKey: " << (int) curKey);
                if (curKey < kLevel) {
                    return PCCompareResults::Smaller;
                } else if (curKey > kLevel) {
                    return PCCompareResults::Bigger;
                }
                ++level;
            }
        }
        return PCCompareResults::Equal;
    }

    typename Tree::PCEqualsResults Tree::checkPrefixEquals(N *n, uint32_t &level, const Key &start, const Key &end,
                                                                            LoadKeyInterface* loadKey) {
        if (n->hasPrefix()) {
            bool endMatches = true;
            Key kt;
            for (uint32_t i = 0; i < n->getPrefixLength(); ++i) {
                if (i == maxStoredPrefixLength) {
                    (*loadKey)(N::getAnyChildTid(n), kt);
                }
                uint8_t startLevel = (start.getKeyLen() > level) ? start[level] : 0;
                uint8_t endLevel = (end.getKeyLen() > level) ? end[level] : 0;

                uint8_t curKey = i >= maxStoredPrefixLength ? kt[level] : n->getPrefix()[i];
                if (curKey > startLevel && curKey < endLevel) {
                    return PCEqualsResults::Contained;
                } else if (curKey < startLevel || curKey > endLevel) {
                    return PCEqualsResults::NoMatch;
                } else if (curKey != endLevel) {
                    endMatches = false;
                }
                ++level;
            }
            if (!endMatches) {
                return PCEqualsResults::StartMatch;
            }
        }
        return PCEqualsResults::BothMatch;
    }

    void Tree::dump() const {
        N::dump(loadKey.get(), root, 0, 0);
    }

    size_t Tree::memory_footprint(){
        return
            _private_num_nodes_N4 * sizeof(N4) +
            _private_num_nodes_N16 * sizeof(N16) +
            _private_num_nodes_N48 * sizeof(N48) +
            _private_num_nodes_N256 * sizeof(N256);
    }
}
