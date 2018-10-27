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

#include "idls_distributions.hpp"

#include <cassert>
#include <memory>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#include "abtree/abtree.hpp"
#include "configuration.hpp"
#include "errorhandling.hpp"
#include "timer.hpp"
#include "third-party/zipf/genzipf.hpp"

using namespace abtree;
using namespace std;

namespace distribution { namespace idls {


/*****************************************************************************
 *                                                                           *
 *   Uniform distribution                                                    *
 *                                                                           *
 *****************************************************************************/
namespace {
    struct UniformDistribution : public Distribution<int64_t> {
        mt19937_64 m_generator;
        uniform_int_distribution<int64_t> m_distribution;

        UniformDistribution(int64_t start, int64_t end, uint64_t seed) :
            m_generator(seed), m_distribution(start, end) {
            if(start > end) throw std::invalid_argument("[InsDel_Uniform_Distribution_Impl] start > end");
        }

        int64_t next() { return m_distribution(m_generator); }

        bool hasNext() const { return true; }
    };
}

/*****************************************************************************
 *                                                                           *
 *   Zipf distribution                                                       *
 *                                                                           *
 *****************************************************************************/
namespace {
    struct ZipfDistribution0 : public Distribution<int64_t> {
        ZipfDistribution m_distribution;
        const int64_t m_start; // inclusive

        ZipfDistribution0(int64_t start, int64_t end, double alpha, uint64_t seed) :
            m_distribution(alpha, end - start +1, seed), m_start(start) { }

        int64_t next() {
            auto value = m_distribution.next(); // in [1, N]
            int64_t key = value -1 + m_start;
            return key;
        }

        bool hasNext() const { return true; }
    };
}

/*****************************************************************************
 *                                                                           *
 *   Sequential distribution                                                 *
 *                                                                           *
 *****************************************************************************/

namespace {
    struct SequentialDistribution : public Distribution<int64_t> {
        int64_t m_value; // inclusive
        SequentialDistribution(int64_t start =1) : m_value(start) { }
        int64_t next() { return m_value++; }
        bool hasNext() const { return true; }
    };
}

/*****************************************************************************
 *                                                                           *
 *   Vector iterator                                                         *
 *                                                                           *
 *****************************************************************************/
namespace {
    template<typename T>
    struct VectorIterator : public Distribution<T> {
        shared_ptr<vector<T>> m_vector;
        size_t m_position;

        VectorIterator<T>(shared_ptr<vector<T>> ptr) : m_vector(ptr), m_position(0) { }

        T next() {
            assert(m_position < m_vector->size());
            return (*m_vector)[m_position++];
        }

        bool hasNext() const { return m_position < m_vector->size(); }
    };
}



/*****************************************************************************
 *                                                                           *
 *   IDLS_Builder                                                            *
 *                                                                           *
 *****************************************************************************/
namespace {

    struct Builder {
        size_t m_counter = 0;
        ABTree m_tree;
        unique_ptr<Distribution<int64_t>> m_distribution_initial_size;
        unique_ptr<Distribution<int64_t>> m_distribution_insert;
        unique_ptr<Distribution<int64_t>> m_distribution_delete;
        shared_ptr<vector<int64_t>> m_insert_keys;
        shared_ptr<vector<int64_t>> m_insdel_keys;
        shared_ptr<vector<int64_t>> m_lookup_keys;
        vector<pair<double, shared_ptr<vector<ScanRange>>>> m_scan_keys;
        unique_ptr<vector<pair<int64_t, int64_t>>> m_prefixes; // to generate lookups & range queries, a vector with all keys & their prefix sum
        const size_t m_initial_size;
        const size_t m_num_ins_del;
        const size_t m_num_lookups;
        const size_t m_inserts_per_group; // first insertions
        const size_t m_deletions_per_group; // then deletions
        const size_t m_seed_lookups;
        const vector<pair<double, size_t>>& m_scan_ranges;
        const bool m_restore_initial_size;

        Builder(eDistributionType init_distribution_type, double init_alpha, uint64_t init_seed,
                eDistributionType insert_distribution_type, double insert_alpha, uint64_t insert_seed,
                eDistributionType delete_distribution_type, double delete_alpha, uint64_t delete_seed,
                size_t distribution_range, uint64_t lookup_seed,
                size_t initial_size, size_t num_ins_del,
                size_t num_inserts_per_group, size_t num_deletions_per_group,
                size_t num_lookups, const vector<pair<double, size_t>>& scan_ranges,
                bool restore_initial_size) :
            m_tree(64), m_initial_size(initial_size), m_num_ins_del(num_ins_del), m_num_lookups(num_lookups),
            m_inserts_per_group(num_inserts_per_group), m_deletions_per_group(num_deletions_per_group),
            m_seed_lookups(lookup_seed), m_scan_ranges(scan_ranges), m_restore_initial_size(restore_initial_size){
            // validate the input
            for(size_t i = 0; i < m_scan_ranges.size(); i++){
                double alpha = m_scan_ranges[i].first;
                size_t num_repetitions = m_scan_ranges[i].second;

                if(alpha <= 0 || alpha > 1)
                    RAISE_EXCEPTION(Exception, "[IDLS_Builder] [" << i << "] Invalid value for alpha: " << alpha);
                if(num_repetitions == 0)
                    RAISE_EXCEPTION(Exception, "[IDLS_Builder] [" << i << "] Invalid value for num_repetitions: " << num_repetitions);
            }

            // create the distributions
            m_distribution_initial_size = initialize_distribution(init_distribution_type, init_alpha, distribution_range, init_seed);
            m_distribution_insert = initialize_distribution(insert_distribution_type, insert_alpha, distribution_range, insert_seed);
            m_distribution_delete = initialize_distribution(delete_distribution_type, delete_alpha, distribution_range, delete_seed);

            Timer timer;
            timer.reset(true);
            m_insert_keys = make_shared<vector<int64_t>>();
            m_insert_keys->reserve(initial_size);
            m_insdel_keys = make_shared<vector<int64_t>>();
            m_insdel_keys->reserve(num_ins_del);
            if(num_lookups > 0){
                m_lookup_keys = make_shared<vector<int64_t>>();
                m_lookup_keys->reserve(num_lookups);
            }
            timer.stop();
            if(timer.milliseconds() > 0){
                LOG_VERBOSE("# IDLSGen, memory allocation time: " << timer.milliseconds() << " milliseconds");
            }


            timer.reset(true);
            generate_initial_inserts();
            timer.stop();
            if(timer.milliseconds() > 0){
                LOG_VERBOSE("# IDLSGen, initial preparation time: " << timer.milliseconds() << " milliseconds")
            }

            timer.reset(true);
            generate_ins_del();
            timer.stop();
            if(timer.milliseconds() > 0){
                LOG_VERBOSE("# IDLSGen, insert/delete preparation time: " << timer.milliseconds() << " milliseconds")
            }

            timer.reset(true);
            generate_lookups();
            timer.stop();
            if(timer.milliseconds() > 0){
                LOG_VERBOSE("# IDLSGen, lookup preparation time: " << timer.milliseconds() << " milliseconds")
            }

            if(m_scan_ranges.size() > 0){
                timer.reset(true);
                generate_scan_ranges();
                timer.stop();

                if(timer.milliseconds() > 0){
                    LOG_VERBOSE("# IDLSGen, scan preparation time: " << timer.milliseconds() << " milliseconds")
                }
            }
        }

        unique_ptr<Distribution<int64_t>> initialize_distribution(eDistributionType distribution_type, double alpha, size_t range, uint64_t seed){
            switch(distribution_type){
            case eDistributionType::uniform:
                return unique_ptr<Distribution<int64_t>>{ new UniformDistribution(1, range, seed) };
                break;
            case eDistributionType::zipf:
                return unique_ptr<Distribution<int64_t>>{ new ZipfDistribution0(1, range, alpha, seed) };
                break;
            case eDistributionType::sequential:
                return unique_ptr<Distribution<int64_t>>{ new SequentialDistribution(alpha) };
            default:
                RAISE_EXCEPTION(Exception, "[IDLS_Builder] Invalid distribution: " << (int) distribution_type);
            }
        }

        void generate_initial_inserts(){
            assert(m_tree.size() == 0);
            for(size_t i = 0; i < m_initial_size; i++){
                auto key = (m_distribution_initial_size->next() << 32) | (m_counter++);
                m_tree.insert(key, 0);
                m_insert_keys->push_back(key);
            }

            assert(m_tree.size() == m_initial_size);
        }

        void generate_ins(size_t count){
            for(size_t i = 0; i < count; i++){
                auto key = (m_distribution_insert->next() << 32) | (m_counter++);
                m_tree.insert(key, 0);
                m_insdel_keys->push_back(key);
            }
        }

        void generate_del(size_t count){
            size_t i = 0;
            while(i < count){
                if(m_tree.size() == 0) return;
                int64_t key = -1;

                // select the next key to remove
                auto candidate_key = m_distribution_delete->next() << 32;
                auto it = m_tree.find(candidate_key, std::numeric_limits<int64_t>::max());
                if(it->hasNext()){
                    key = it->next().first;
                } else {
                    key = m_tree.key_max();
                }

#if !defined(NDEBUG) // sanity check
                assert(m_tree.find(key) == 0);
                size_t sz_before = m_tree.size();
#endif

                assert(m_tree.find(key) == 0 && "The key should be present at this stage as it hasn't been removed yet");
                m_tree.remove(key);
                assert(m_tree.find(key) == -1 && "The key has just been removed and there should not be duplicates");
                m_insdel_keys->push_back(-key); // minus implies a delete op.

#if !defined(NDEBUG) // sanity check
                size_t sz_after = m_tree.size();
                assert(sz_after +1 == sz_before);
#endif
                i++;
            }
        }

        void generate_ins_del(){
            size_t count = 0;
            size_t total = m_num_ins_del;
            while( count < total ){
                generate_ins(m_inserts_per_group);
                generate_del(m_deletions_per_group);

                count += m_inserts_per_group + m_deletions_per_group;
            }

            // go back to the initial size
            if(m_restore_initial_size){
                int64_t diff = ((int64_t) m_initial_size) - ((int64_t) m_tree.size());
                if( diff >= 0 ){
                    generate_ins(diff);
                } else {
                    generate_del(-diff);
                }

                assert(m_tree.size() == m_initial_size);
            }
        }

        void generate_prefix_sum(){
            m_prefixes.reset(new vector<pair<int64_t, int64_t>>{});
            auto& prefixes = (*m_prefixes.get());

            prefixes.reserve(m_tree.size());

            // retrieve the keys & compute the prefix sum
            size_t j = 0;
            auto it = m_tree.iterator();
            while(it->hasNext()){
                auto key = it->next().first;
                auto prefix_sum = key;
                if(j > 0) prefix_sum += prefixes[j-1].second;

                prefixes.emplace_back(key, prefix_sum);

                j++;
            }
            assert(prefixes.size() == m_tree.size());
        }

        void generate_lookups(){
            if(m_tree.size() == 0) return;

            if(m_prefixes.get() == nullptr) generate_prefix_sum();
            auto& prefixes = (*m_prefixes.get());

            mt19937_64 random_generator{m_seed_lookups};
            std::uniform_int_distribution<int64_t> distribution(0, m_tree.size() -1);


            for(size_t i = 0; i < m_num_lookups; i++){
                m_lookup_keys->push_back(prefixes[distribution(random_generator)].first);
            }
        }

        void generate_scan_ranges(){
            if(m_tree.size() == 0) return;
            if(m_prefixes.get() == nullptr) generate_prefix_sum();
            auto& prefixes = (*m_prefixes.get());

            // generate the scans
            mt19937_64 random_generator{m_seed_lookups + 1658};

            for(size_t i = 0; i < m_scan_ranges.size(); i++){
                double interval_sz = m_scan_ranges[i].first;
                size_t num_repetitions = m_scan_ranges[i].second;
                m_scan_keys.emplace_back(interval_sz, make_shared<vector<ScanRange>>());
                auto& vector = *(m_scan_keys.back().second.get());

                size_t length = static_cast<int64_t>(interval_sz * prefixes.size());
                if(length < 1) length = 1;

        //        std::cout << "interval_size: " << interval_sz << ", length: " << length << "\n";
                size_t rstart = 1;
                size_t rend = prefixes.size() - length;
                if(rend < rstart) { rend = rstart; length = prefixes.size() - rstart; } // [1, 1]
                std::uniform_int_distribution<int64_t> distribution(rstart, rend);

                for(size_t i = 0; i < num_repetitions; i++){
                    int64_t outcome = distribution(random_generator);
                    auto index_min = outcome;
                    auto index_max = outcome + length -1;
                    assert(index_max < prefixes.size());
                    auto value_min = prefixes[index_min].first;
                    auto value_max = prefixes[index_max].first;
                    uint64_t expected_sum = prefixes[index_max].second;
                    if(index_min > 0) expected_sum -= prefixes[index_min -1].second;

                    vector.push_back({value_min, value_max, expected_sum});
                }
            }
        }
    };

} // anonymous namespace

/*****************************************************************************
 *                                                                           *
 *   Generator                                                               *
 *                                                                           *
 *****************************************************************************/
Generator::Generator() :
        m_init_distribution_type(eDistributionType::uniform),
        m_insert_distribution_type(eDistributionType::uniform),
        m_delete_distribution_type(eDistributionType::uniform) { }

void Generator::set_initial_size(size_t value) {
    if(value == 0) RAISE_EXCEPTION(Exception, "The given value must be greater than 0: " << value);
    m_num_initial_inserts = value;
}

void Generator::set_insdel(size_t total, size_t group_size){
    if(group_size == 0) RAISE_EXCEPTION(Exception, "group_size == 0");

    m_num_ins_del = total;
    m_num_insertions_per_batch = m_num_deletions_per_batch = group_size;
}

void Generator::set_lookups(size_t total){
    m_num_lookups = total;
}

void Generator::set_scans(vector<pair<double, size_t>>& scan_vector){
    for(size_t i = 0; i < scan_vector.size(); i++){
        auto& p = scan_vector[i];
        double alpha = p.first;
        if(alpha <= 0 || alpha > 1)
            RAISE_EXCEPTION(Exception, "[" << i << "] Invalid value for the interval size: " << alpha << ". It must be a value in (0, 1]");
        int64_t num_repetitions = p.second;
        if(num_repetitions == 0)
            RAISE_EXCEPTION(Exception, "[" << i << "] Interval: " << alpha << ". Invalid value for the number of repetitions: 0");
    }

    m_num_scans = scan_vector;
}

void Generator::set_seed(uint64_t value){
    m_seed = value;
}

void Generator::set_distribution_type_init(eDistributionType value, double alpha){
    if(value == eDistributionType::zipf && (alpha <= 0)) // otherwise it's simply ignored
        RAISE_EXCEPTION(Exception, "[zipf distribution] Cannot set the parameter --alpha <= 0: " << alpha);

    m_init_distribution_type = value;
    m_init_alpha = alpha;
}

void Generator::set_distribution_type_insert(eDistributionType value, double alpha){
    if(value == eDistributionType::zipf && (alpha <= 0)) // otherwise it's simply ignored
        RAISE_EXCEPTION(Exception, "[zipf distribution] Cannot set the parameter --alpha <= 0: " << alpha);
//    if(beta < 1)
//        RAISE_EXCEPTION(Exception, "Cannot set the parameter --beta < 1: " << beta << ". It determines the window of generated values.");

    m_insert_distribution_type = value;
    m_insert_alpha = alpha;
}

void Generator::set_distribution_type_delete(eDistributionType value, double alpha){
    if(value == eDistributionType::zipf && (alpha <= 0)) // otherwise it's simply ignored
        RAISE_EXCEPTION(Exception, "[zipf distribution] Cannot set the parameter --alpha <= 0: " << alpha);

    m_delete_distribution_type = value;
    m_delete_alpha = alpha;
}

void Generator::set_distribution_range(size_t beta){
    m_distribution_range = beta;
}

void Generator::set_restore_initial_size(bool value){
    m_restore_initial_size = value;
}

DistributionsContainer Generator::generate() const {
    // validate the parameters
    if(m_num_initial_inserts == 0)
        RAISE_EXCEPTION(Exception, "[Generator::generate] Number of initial inserts not set");
    if(m_distribution_range < 1 && (m_init_distribution_type == eDistributionType::zipf || m_insert_distribution_type == eDistributionType::zipf || m_delete_distribution_type == eDistributionType::zipf) ){
        RAISE_EXCEPTION(Exception, "Cannot set the parameter --beta < 1: " << m_distribution_range << ". It determines the window of generated values.");
    }

    LOG_VERBOSE("IDLS - Initial size: " << m_num_initial_inserts << ", insert/deletes: " << m_num_ins_del << " in groups of " << m_num_insertions_per_batch << " inserts and " << m_num_deletions_per_batch << " deletes, lookups: " << m_num_lookups << ", scan intervals: " << m_num_scans.size() << ", restore initial size: " << m_restore_initial_size);

    Builder builder{
        m_init_distribution_type, m_init_alpha, m_seed + 2397697948782407152,
        m_insert_distribution_type, m_insert_alpha, m_seed + 8078853756375799745,
        m_delete_distribution_type, m_delete_alpha, m_seed + 8472267212,
        m_distribution_range, m_seed + 3410700567427482686,
        m_num_initial_inserts, m_num_ins_del, m_num_insertions_per_batch, m_num_deletions_per_batch, m_num_lookups, m_num_scans,
        m_restore_initial_size
    };

    DistributionsContainer result;
    result.m_keys_initial_size = builder.m_insert_keys;
    result.m_keys_insdel = builder.m_insdel_keys;
    result.m_keys_lookup = builder.m_lookup_keys;
    result.m_keys_scan = builder.m_scan_keys;

    return result;
}


/*****************************************************************************
 *                                                                           *
 *   DistributionsContainer                                                  *
 *                                                                           *
 *****************************************************************************/
DistributionsContainer::DistributionsContainer() { }

unique_ptr<Distribution<int64_t>> DistributionsContainer::preparation_step(){
    return make_unique<VectorIterator<int64_t>>( m_keys_initial_size );
}

void DistributionsContainer::unset_preparation_step(){
    m_keys_initial_size.reset();
}

unique_ptr<Distribution<int64_t>> DistributionsContainer::insdel_step(){
    return make_unique<VectorIterator<int64_t>>( m_keys_insdel );
}

void DistributionsContainer::unset_insdel_step(){
    m_keys_insdel.reset();
}

unique_ptr<Distribution<int64_t>> DistributionsContainer::lookup_step(){
    return make_unique<VectorIterator<int64_t>>( m_keys_lookup );
}

vector<pair<double, unique_ptr<Distribution<ScanRange>>>> DistributionsContainer::scan_step(){
    vector<pair<double, unique_ptr<Distribution<ScanRange>>>> result;
    result.reserve(m_keys_scan.size());
    for(size_t i = 0; i < m_keys_scan.size(); i++){
        auto& p = m_keys_scan.at(i);
        result.emplace_back(p.first, new VectorIterator<ScanRange>(p.second));
    }

    return result;
}

}} // namespace distribution::idls

