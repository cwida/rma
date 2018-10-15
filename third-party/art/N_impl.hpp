#include <assert.h>
#include <algorithm>
#include <iomanip>
#include <iostream>

#include "N.h"

#include "N4_impl.hpp"
#include "N16_impl.hpp"
#include "N48_impl.hpp"
#include "N256_impl.hpp"

namespace ART_unsynchronized {

    void N::setType(NTypes type) {
        this->type = type;
    }

    NTypes N::getType() const {
        return type;
    }

    N *N::getAnyChild(const N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                return n->getAnyChild();
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                return n->getAnyChild();
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    void N::change(N *node, uint8_t key, N *val) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                n->change(key, val);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->change(key, val);
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename biggerN>
    void N::insertGrow(curN *n, N *parentNode, uint8_t keyParent, uint8_t key, N *val) {
        if (n->insert(key, val)) {
            return;
        }

        auto nBig = new biggerN(n->getPrefix(), n->getPrefixLength());
        n->copyTo(nBig);
        nBig->insert(key, val);

        N::change(parentNode, keyParent, nBig);

        delete n;
    }

    void N::insertA(N *node, N *parentNode, uint8_t keyParent, uint8_t key, N *val) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                insertGrow<N4, N16>(n, parentNode, keyParent, key, val);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                insertGrow<N16, N48>(n, parentNode, keyParent, key, val);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                insertGrow<N48, N256>(n, parentNode, keyParent, key, val);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->insert(key, val);
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    N *N::getChild(const uint8_t k, N *node) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getChild(k);
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                return n->getChild(k);
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                return n->getChild(k);
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                return n->getChild(k);
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    void N::deleteChildren(N *node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                n->deleteChildren();
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                n->deleteChildren();
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    template<typename curN, typename smallerN>
    void N::removeAndShrink(curN *n, N *parentNode, uint8_t keyParent, uint8_t key) {
        if (n->remove(key, parentNode == nullptr)) {
            return;
        }

        auto nSmall = new smallerN(n->getPrefix(), n->getPrefixLength());


        n->remove(key, true);
        n->copyTo(nSmall);
        N::change(parentNode, keyParent, nSmall);

        delete n;
    }

    void N::removeA(N *node, uint8_t key, N *parentNode, uint8_t keyParent) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                n->remove(key, false);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                removeAndShrink<N16, N4>(n, parentNode, keyParent, key);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                removeAndShrink<N48, N16>(n, parentNode, keyParent, key);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                removeAndShrink<N256, N48>(n, parentNode, keyParent, key);
                return;
            }
        }
        assert(false);
        __builtin_unreachable();
    }

    uint32_t N::getPrefixLength() const {
        return prefixCount;
    }

    bool N::hasPrefix() const {
        return prefixCount > 0;
    }

    uint32_t N::getCount() const {
        return count;
    }

    const uint8_t *N::getPrefix() const {
        return prefix;
    }

    void N::setPrefix(const uint8_t *prefix, uint32_t length) {
        if (length > 0) {
            memcpy(this->prefix, prefix, std::min(length, maxStoredPrefixLength));
            prefixCount = length;
        } else {
            prefixCount = 0;
        }
    }

    void N::addPrefixBefore(N *node, uint8_t key) {
        uint32_t prefixCopyCount = std::min(maxStoredPrefixLength, node->getPrefixLength() + 1);
        memmove(this->prefix + prefixCopyCount, this->prefix,
                std::min(this->getPrefixLength(), maxStoredPrefixLength - prefixCopyCount));
        memcpy(this->prefix, node->prefix, std::min(prefixCopyCount, node->getPrefixLength()));
        if (node->getPrefixLength() < maxStoredPrefixLength) {
            this->prefix[prefixCopyCount - 1] = key;
        }
        this->prefixCount += node->getPrefixLength() + 1;
    }


    bool N::isLeaf(const N *n) {
        return (reinterpret_cast<uint64_t>(n) & (static_cast<uint64_t>(1) << 63)) == (static_cast<uint64_t>(1) << 63);
    }

    N *N::setLeaf(TID tid) {
        return reinterpret_cast<N *>(tid | (static_cast<uint64_t>(1) << 63));
    }

    TID N::getLeaf(const N *n) {
        return (reinterpret_cast<uint64_t>(n) & ((static_cast<uint64_t>(1) << 63) - 1));
    }

    std::tuple<N *, uint8_t> N::getSecondChild(N *node, const uint8_t key) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                return n->getSecondChild(key);
            }
            default: {
                assert(false);
                __builtin_unreachable();
            }
        }
    }

    void N::deleteNode(N *node) {
        if (N::isLeaf(node)) {
            return;
        }
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<N4 *>(node);
                delete n;
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<N16 *>(node);
                delete n;
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<N48 *>(node);
                delete n;
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<N256 *>(node);
                delete n;
                return;
            }
        }
        delete node;
    }


    TID N::getAnyChildTid(N *n) {
        N *nextNode = n;
        N *node = nullptr;

        nextNode = getAnyChild(nextNode);

        assert(nextNode != nullptr);
        if (isLeaf(nextNode)) {
            return getLeaf(nextNode);
        }

        while (true) {
            node = nextNode;

            nextNode = getAnyChild(node);

            assert(nextNode != nullptr);
            if (isLeaf(nextNode)) {
                return getLeaf(nextNode);
            }
        }
    }

    TID N::getMaxLeaf(N* node){
        assert(node != nullptr);

        while(!isLeaf(node)){
            switch (node->getType()) {
            case NTypes::N4:
                node = static_cast<N4*>(node)->getMaxChild();
                break;
            case NTypes::N16:
                node = static_cast<N16 *>(node)->getMaxChild();
                break;
            case NTypes::N48:
                node = static_cast<N48 *>(node)->getMaxChild();
                break;
            case NTypes::N256:
                node = static_cast<N256 *>(node)->getMaxChild();
                break;
            }
            assert(node != nullptr);
        }

        return getLeaf(node);
    }

    N* N::getChildLessOrEqual(N* node, uint8_t key, bool* out_exact_match){
        bool flag_ignore;
        bool& flag_exact_match = (out_exact_match != nullptr) ? *out_exact_match : flag_ignore;
//        COUT_DEBUG("N::getChildLessOrEqual, key: 0x" << std::hex << (int) key << std::dec);

        assert(node != nullptr);
        switch (node->getType()) {
        case NTypes::N4:
            return static_cast<N4*>(node)->getChildLessOrEqual(key, flag_exact_match);
        case NTypes::N16:
            return static_cast<N16 *>(node)->getChildLessOrEqual(key, flag_exact_match);
        case NTypes::N48:
            return static_cast<N48 *>(node)->getChildLessOrEqual(key, flag_exact_match);
        case NTypes::N256:
            return static_cast<N256 *>(node)->getChildLessOrEqual(key, flag_exact_match);
        }

        assert(false);
        __builtin_unreachable();
    }

    N* N::getPredecessor(N* node, uint8_t key){
        assert(node != nullptr);
        if(key > 0) {
            return getChildLessOrEqual(node, key -1, nullptr);
        } else
            return nullptr;
    }

    void N::getChildren(const N *node, uint8_t start, uint8_t end, std::tuple<uint8_t, N *> children[],
                        uint32_t &childrenCount) {
        switch (node->getType()) {
            case NTypes::N4: {
                auto n = static_cast<const N4 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N16: {
                auto n = static_cast<const N16 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N48: {
                auto n = static_cast<const N48 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
            case NTypes::N256: {
                auto n = static_cast<const N256 *>(node);
                n->getChildren(start, end, children, childrenCount);
                return;
            }
        }
    }

    void N::print_tabs(int depth){
        using namespace std;
        auto flags = cout.flags();
        cout << setw(depth * 4) << setfill(' ') << ' ';
        cout.setf(flags);
    }

    void N::dump(LoadKeyInterface* loadKey, N* node, int level, int depth){
        using namespace std;
        assert(node != nullptr);

        print_tabs(depth);
        if(isLeaf(node)){
            Key key;
            auto tid = getLeaf(node);
            (*loadKey)(tid, key);
            cout << "Leaf: " << node << ", tid: " << tid << ", key: " << key << endl;
        } else {
            cout << "Node: " << node << ", level: " << level << ", type: ";
            switch(node->getType()){
            case NTypes::N4: cout << "N4"; break;
            case NTypes::N16: cout << "N16"; break;
            case NTypes::N48: cout << "N48"; break;
            case NTypes::N256: cout << "N256"; break;
            }
            cout << " (" << static_cast<int>(node->getType()) << ")";
            cout << ", count: " << node->getCount() << "\n";

            // prefix
            print_tabs(depth);
            cout << "Prefix, length: " << node->getPrefixLength();
            for(int i = 0, sz = node->getPrefixLength(); i < sz; i++){
                cout << ", " << i << ": 0x" << std::hex << static_cast<int64_t>(node->getPrefix()[i]) << std::dec;
            }
            cout << "\n";

            // children
            tuple<uint8_t, N *> children[256];
            uint32_t childrenCount(0);
            node->getChildren(node, 0, 255, children, childrenCount);
            print_tabs(depth);
            cout << "Children: " << childrenCount;
            for(int i = 0; i < childrenCount; i++){
                cout << ", {byte:" << static_cast<int>(get<0>(children[i])) << ", pointer:" << get<1>(children[i]) << "}";
            }
            cout << endl;

            for(int i = 0; i < childrenCount; i++){
                dump(loadKey, get<1>(children[i]), level + 1 + node->getPrefixLength(), depth +1);
            }
        }
    }
}
