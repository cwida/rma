#include <assert.h>
#include <algorithm>
#include "N.h"

namespace ART_unsynchronized {

    N48::N48(const uint8_t *prefix, uint32_t prefixLength) : N(NTypes::N48, prefix,
                                                                          prefixLength) {
        memset(childIndex, emptyMarker, sizeof(childIndex));
        memset(children, 0, sizeof(children));
        _private_num_nodes_N48++; // to compute the memory footprint
    }

    N48::~N48(){
        assert(_private_num_nodes_N48 > 0);
        _private_num_nodes_N48--;
    }

    bool N48::insert(uint8_t key, N *n) {
        if (count == 48) {
            return false;
        }
        unsigned pos = count;
        if (children[pos]) {
            for (pos = 0; children[pos] != nullptr; pos++);
        }
        children[pos] = n;
        childIndex[key] = (uint8_t) pos;
        count++;
        return true;
    }

    template<class NODE>
    void N48::copyTo(NODE *n) const {
        for (unsigned i = 0; i < 256; i++) {
            if (childIndex[i] != emptyMarker) {
                n->insert(i, children[childIndex[i]]);
            }
        }
    }

    void N48::change(uint8_t key, N *val) {
        children[childIndex[key]] = val;
    }

    N *N48::getChild(const uint8_t k) const {
        if (childIndex[k] == emptyMarker) {
            return nullptr;
        } else {
            return children[childIndex[k]];
        }
    }

    bool N48::remove(uint8_t k, bool force) {
        if (count == 12 && !force) {
            return false;
        }
        assert(childIndex[k] != emptyMarker);
        children[childIndex[k]] = nullptr;
        childIndex[k] = emptyMarker;
        count--;
        assert(getChild(k) == nullptr);
        return true;
    }

    N *N48::getAnyChild() const {
        N *anyChild = nullptr;
        for (unsigned i = 0; i < 256; i++) {
            if (childIndex[i] != emptyMarker) {
                if (N::isLeaf(children[childIndex[i]])) {
                    return children[childIndex[i]];
                } else {
                    anyChild = children[childIndex[i]];
                };
            }
        }
        return anyChild;
    }

    N *N48::getMaxChild() const {
        for (int i = 255; i>= 0; i--) {
            if(childIndex[i] != emptyMarker){
                return children[childIndex[i]];
            }
        }

        assert(0 && "This code should be unreachable!");
        return nullptr;
    }

    N* N48::getChildLessOrEqual(uint8_t key, bool& out_exact_match) const {
//        std::cout << "[N48::getChildLessOrEqual] node: " << this << ", key: " << (unsigned) key << std::endl;
        int index = key; // convert the type
        out_exact_match = (childIndex[index] != emptyMarker);
        if(out_exact_match){
            return children[childIndex[index]];
        } else {
            index--;
            while(index >= 0){
                if(childIndex[index] != emptyMarker){
                    return children[childIndex[index]];
                }
                index--;
            }
            return nullptr;
        }
    }

    void N48::deleteChildren() {
        for (unsigned i = 0; i < 256; i++) {
            if (childIndex[i] != emptyMarker) {
                N::deleteChildren(children[childIndex[i]]);
                N::deleteNode(children[childIndex[i]]);
            }
        }
    }

    void N48::getChildren(uint8_t start, uint8_t end, std::tuple<uint8_t, N *> *&children,
                          uint32_t &childrenCount) const {
        childrenCount = 0;
        for (unsigned i = start; i <= end; i++) {
            if (this->childIndex[i] != emptyMarker) {
                children[childrenCount] = std::make_tuple(i, this->children[this->childIndex[i]]);
                childrenCount++;
            }
        }
    }
}
