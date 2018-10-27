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

#include "random_permutation.hpp"

#include <cassert>
#include <cmath>
#include <cstring> // memcpy
#include <future>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <type_traits> // decay

#include "console_arguments.hpp"
#include "miscellaneous.hpp"

using namespace std;

namespace distribution {

RandomPermutation::RandomPermutation() { }
RandomPermutation::~RandomPermutation() { }

/******************************************************************************
 *                                                                            *
 *  RandomPermutationLegacy                                                   *
 *                                                                            *
 ******************************************************************************/

RandomPermutationLegacy::RandomPermutationLegacy(const size_t size) : sz(size) {
    auto seed = ARGREF(uint64_t, "seed_random_permutation").get();
    unique_ptr<pair<int64_t, int64_t>[]> ptr(new pair<int64_t, int64_t>[size]);
    pair<int64_t, int64_t>* elts = ptr.get();

    using random_generator_t = mt19937_64;
    random_generator_t random_generator{seed};
    pair<int64_t, int64_t>* __restrict A = elts;
    A[0] = std::pair<int64_t, int64_t>{1, 10}; // init the first position
    for(size_t i = 1; i < sz; i++){
        size_t j = uniform_int_distribution<size_t>{0, i -1}(random_generator);
        A[i] = A[j];
        A[j] = std::pair<int64_t, int64_t>{i +1, (i+1) * 10}; // ptr to A[i];
    }

    el_ptr = std::move(ptr);
}
RandomPermutationLegacy::~RandomPermutationLegacy() {  }
size_t RandomPermutationLegacy::size() const { return sz; }
KeyValue RandomPermutationLegacy::get(size_t index) const {
    auto el = el_ptr.get();
    return el[index];
}

static size_t compute_bytes_per_elements(size_t sz){
    double bits = ceil(log2(sz));
    size_t bytes = max((int) ceil(bits / 8), 1); // keep at least one byte
//    cout << "[compute_bytes_per_elements] sz: " << sz << ", bits: " << bits << ", bytes: " << bytes << endl;
    return bytes;
}

/******************************************************************************
 *                                                                            *
 *  RandomPermutationCompressed                                               *
 *                                                                            *
 ******************************************************************************/
RandomPermutationCompressed::RandomPermutationCompressed(size_t size, uint64_t seed) : array(compute_bytes_per_elements(size), size) {
    do_permutation(seed);
}

RandomPermutationCompressed::~RandomPermutationCompressed(){ }

void RandomPermutationCompressed::do_permutation(uint64_t seed){
    using random_generator_t = mt19937_64;
    random_generator_t random_generator{seed};

    array[0] = 0; // init the first position
    for(size_t i = 1, sz = size(); i < sz; i++){
        size_t j = uniform_int_distribution<size_t>{0, i -1}(random_generator);
        array[i] = array[j];
        array[j] = i;
    }
}

size_t RandomPermutationCompressed::size() const {
    return array.capacity();
}

KeyValue RandomPermutationCompressed::get(size_t index) const {
    if(index > size()){
        throw std::invalid_argument("Index out of bounds");
    }
    int64_t key = array[index] +1;
    return {key, key*10};
}

/******************************************************************************
 *                                                                            *
 *  RandomPermutationParallel                                                 *
 *                                                                            *
 ******************************************************************************/

#if 0
static mutex g_local_mutex;
#define SAFE_COUT(msg) { lock_guard<mutex> lock{g_local_mutex}; cout << "[Thread " << this_thread::get_id() << "] " << msg << endl; }
#else
#define SAFE_COUT(msg)
#endif

namespace {
    struct Bucket{
        using random_generator_t = mt19937_64;

        const int bucket_no;
        random_generator_t random_generator;
        vector<int64_t>** chunks;
        const int chunks_size;
        CByteArray* permutation;

        Bucket(int id, uint64_t seed, uint64_t no_chucks, size_t bytes_per_element) : bucket_no(id), random_generator(seed + bucket_no),
                chunks(nullptr), chunks_size(no_chucks), permutation(nullptr){
            SAFE_COUT("[Bucket::Bucket] bucket_id: " << bucket_no);
            // initialise the chunks
            chunks = new vector<int64_t>*[chunks_size];
            for(int i = 0; i < chunks_size; i++){
                chunks[i] = new vector<int64_t>();
            }
        }

        ~Bucket(){
            SAFE_COUT("[Bucket::~Bucket] bucket_id: " << bucket_no);
           if(chunks){
               for(int i = 0; i < chunks_size; i++){
                   delete chunks[i]; chunks[i] = nullptr;
               }
           }
           delete[] chunks; chunks = nullptr;

           delete permutation; permutation = nullptr;
        }
    };

    // static function
    static unique_ptr<CByteArray> compute_random_permutation_parallel(size_t size, uint64_t no_buckets, uint64_t seed){
        if(no_buckets > size) no_buckets = size;

        // initialise the buckets
        std::vector<std::unique_ptr<Bucket>> buckets;
        size_t bytes_per_element = compute_bytes_per_elements(size);
        for(uint64_t i = 0; i < no_buckets; i++){
            buckets.emplace_back(new Bucket(i, seed + i, no_buckets, bytes_per_element));
        }

        { // exchange the elements in the buckets
            auto create_partition = [&buckets](int bucket_id, uint64_t range_start /* inclusive */ , uint64_t range_end /* exclusive */){
                assert(bucket_id < (int) buckets.size());
                SAFE_COUT("[create_partition] bucket_id: " << bucket_id << ", range_start: " << range_start << ", range_end: " << range_end);

                // first gather all the buckets
                std::vector< std::vector<int64_t>* > stores;
                for(size_t i = 0; i < buckets.size(); i++){
                    Bucket* b = buckets[i].get();
                    stores.push_back( b->chunks[bucket_id] );
                }

                // current state
                Bucket* bucket = buckets[bucket_id].get();
                uniform_int_distribution<size_t> distribution{0, buckets.size() -1};
                for(uint64_t i = range_start; i < range_end; i++){
                    size_t target_bucket = distribution(bucket->random_generator);
                    stores[target_bucket]->push_back(i);
                }
            };

            std::vector<future<void>> tasks;
            size_t range_start = 0, range_step = size / no_buckets, range_end = range_step;
            uint64_t range_mod = size % no_buckets;
            SAFE_COUT("range_mod:" << range_mod);
            for(uint64_t i = 0; i < no_buckets; i++){
                if(i < range_mod) range_end++;
                tasks.push_back( async(launch::async, create_partition, i, range_start, range_end) );
                range_start = range_end;
                range_end += range_step;
            }
            // wait for all tasks to finish
            for(auto& t: tasks) t.get();
        }


        { // perform a local permutation
            auto local_permutation = [&buckets, bytes_per_element](int bucket_id){
                // first store all the values in a single array
                Bucket* bucket = buckets[bucket_id].get();
                size_t capacity = 0;
                assert((size_t) bucket->chunks_size == buckets.size());
                for(int i = 0; i < bucket->chunks_size; i++){
                    capacity += bucket->chunks[i]->size();
                }
                SAFE_COUT("[local_permutation] bucket_id: " << bucket_id << ", capacity: " << capacity << ", bytes_per_element: " << bytes_per_element);
                bucket->permutation = new CByteArray(bytes_per_element, capacity);
                size_t index = 0;
                for(int i = 0; i < bucket->chunks_size; i++){
                    std::vector<int64_t>* vector = bucket->chunks[i];
                    for(size_t j = 0, sz = vector->size(); j < sz; j++){
                        bucket->permutation->set_value_at(index++, vector->at(j));
                    }
                }

                // deallocate the chunks to save some memory
                for(int i = 0; i < bucket->chunks_size; i++){
                    delete bucket->chunks[i];
                    bucket->chunks[i] = nullptr;
                }
                delete[] bucket->chunks; bucket->chunks = nullptr;

                // perform a local permutation
                if(capacity >= 2){
                    CByteArray& local = *(bucket->permutation);
                    for(size_t i = 0; i < capacity -2; i++){
                        size_t j = uniform_int_distribution<size_t>{i, capacity -1}(bucket->random_generator);
                        // swap A[i] with A[j]
                        int64_t tmp = local[i];
                        local[i] = local[j];
                        local[j] = tmp;
                    }
                }
            };

            std::vector<future<void>> tasks;
            for(size_t i = 0; i < no_buckets; i++){
                tasks.push_back( async(launch::async, local_permutation, i) );
            }
            // wait for all tasks to finish
            for(auto& t: tasks) t.get();
        }


        // finally concatenate the results
        assert(no_buckets == buckets.size());
        unique_ptr<CByteArray*[]> partitions_ptr(new CByteArray*[no_buckets]);
        auto partitions = partitions_ptr.get();
        for(size_t i = 0; i < buckets.size(); i++){
            assert(buckets[i]->permutation != nullptr);
            partitions[i] = buckets[i]->permutation;
        }
        return CByteArray::merge((decay<decltype(partitions)>::type) partitions, no_buckets);
    }
}


RandomPermutationParallel::RandomPermutationParallel() : container( new CByteArray(1, 0) ){ }

RandomPermutationParallel::RandomPermutationParallel(size_t size, uint64_t seed) : container( compute_random_permutation_parallel(size, 1024, seed) ) { }

RandomPermutationParallel::~RandomPermutationParallel() { }

void RandomPermutationParallel::compute(size_t size, uint64_t seed){
    container = compute_random_permutation_parallel(size, 1024, seed);
}

size_t RandomPermutationParallel::size() const {
    return container->capacity();
}

KeyValue RandomPermutationParallel::get(size_t index) const{
    if(index > size()){
        throw std::invalid_argument("Index out of bounds");
    }
    int64_t key = (*container)[index] +1;
    return {key, key*10};
}

int64_t RandomPermutationParallel::get_raw_key(size_t index) const {
    return (*container)[index];
}

shared_ptr<CByteArray> RandomPermutationParallel::get_container() const { return  container; }
unique_ptr<CByteView> RandomPermutationParallel::get_view() { return get_view(0); }
unique_ptr<CByteView> RandomPermutationParallel::get_view(size_t shift) { return get_view(shift, size() - shift); }
unique_ptr<CByteView> RandomPermutationParallel::get_view(size_t start, size_t length) {
    if(start + length > container->capacity()) throw std::invalid_argument("Index out of bound");
    return make_unique<CByteView>(container, start, start + length);
}

} // namespace distribution
