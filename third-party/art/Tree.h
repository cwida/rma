//
// Created by florian on 18.11.15.
//

#ifndef ARTVERSION1_TREE_H
#define ARTVERSION1_TREE_H
#include "N.h"

#include <memory>

//using namespace ART;

namespace ART_unsynchronized {

    struct LoadKeyInterface {
        virtual ~LoadKeyInterface(){}
        virtual void operator() (const TID tid, Key& k) = 0;
    };

    class Tree {
    public:
//        using LoadKeyFunction = void (*)(TID tid, Key &key);

    private:

        N *const root;

        TID checkKey(const TID tid, const Key &k) const;

        std::shared_ptr<LoadKeyInterface> loadKey;

        enum class CheckPrefixResult : uint8_t {
            Match,
            NoMatch,
            OptimisticMatch
        };

        enum class CheckPrefixPessimisticResult : uint8_t {
            Match,
            NoMatch,
        };

        enum class PCCompareResults : uint8_t {
            Smaller,
            Equal,
            Bigger,
        };
        enum class PCEqualsResults : uint8_t {
            StartMatch,
            BothMatch,
            Contained,
            NoMatch,
        };
        static CheckPrefixResult checkPrefix(N* n, const Key &k, uint32_t &level);

        static CheckPrefixPessimisticResult checkPrefixPessimistic(N *n, const Key &k, uint32_t &level,
                                                                   uint8_t &nonMatchingKey,
                                                                   Prefix &nonMatchingPrefix,
                                                                   LoadKeyInterface* loadKey);

        static PCCompareResults checkPrefixCompare(N* n, const Key &k, uint32_t &level, LoadKeyInterface* loadKey);

        static PCEqualsResults checkPrefixEquals(N* n, uint32_t &level, const Key &start, const Key &end, LoadKeyInterface* loadKey);


        bool findLessOrEqual(const Key& key, N* node, uint32_t level, TID* output_result) const;

    public:

        Tree(std::shared_ptr<LoadKeyInterface> loadKey);

        Tree(const Tree &) = delete;

        Tree(Tree &&t) : root(t.root), loadKey(t.loadKey) { }

        ~Tree();

        TID lookup(const Key &k) const;

        // Find the greatest element less or equal than `key'
        TID findLessOrEqual(const Key& key) const;

        void insert(const Key &k, TID tid);

        void remove(const Key &k, TID tid);

        void dump() const;

        static size_t memory_footprint();
    };
}
#endif //ARTVERSION1_SYNCHRONIZEDTREE_H
