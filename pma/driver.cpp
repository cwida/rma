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

#include "driver.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "errorhandling.hpp"
#include "experiment.hpp"
#include "external.hpp"
#include "factory.hpp"
#include "interface.hpp"
#include "miscellaneous.hpp"

#include "experiments/aging.hpp"
#include "experiments/bandwidth_idls.hpp"
#include "experiments/bulk_loading.hpp"
#include "experiments/idls.hpp"
#include "experiments/insert_lookup.hpp"
#include "experiments/range_query.hpp"
#include "experiments/step_idls.hpp"
#include "experiments/step_insert_lookup.hpp"
#include "experiments/step_insert_scan.hpp"

#include "adaptive/basic/apma_baseline.hpp"

#include "adaptive/bh07_v2/packed_memory_array.hpp"

#include "adaptive/int1/knobs.hpp"
#include "adaptive/int1/packed_memory_array.hpp"
#include "adaptive/int2/packed_memory_array.hpp"
#include "adaptive/int3/packed_memory_array.hpp"

#include "abtree/abtree.hpp"
#include "abtree/art.hpp"
#include "abtree/dense_array.hpp"
#include "abtree/stx-btree.hpp"

#include "btree/btreepma_v2.hpp"
#include "btree/btreepma_v4.hpp"
#include "btree/btreepmacc5.hpp"
#include "btree/btreepmacc7.hpp"
#include "btree/08/packed_memory_array.hpp"

#include "external/dfr/dfr.hpp"
#include "external/iejoin/khayyat.hpp"
#include "external/sha/pma.hpp"
#include "sequential/pma_v4.hpp"

using namespace std;

namespace pma {

static bool initialised = false;

void initialise() {
//    if(initialised) RAISE_EXCEPTION(Exception, "Function pma::initialise() already called once");
    if(initialised) return;

    PARAMETER(uint64_t, "inode_block_size").alias("iB");
    PARAMETER(uint64_t, "leaf_block_size").alias("lB");
    PARAMETER(uint64_t, "extent_size").descr("The size of an extent used for memory rewiring. It is defined as a multiple in terms of a page size.");

    /**
     * Basic PMA implementations
     */
    REGISTER_PMA("pma_jraizes", "GitHub implementation by Justin Raizes", [](){
        return make_unique<PMA_Raizes>();
    });
    REGISTER_PMA("pma_pabmont", "GitHub implementation by Pablo Montes", [](){
        return make_unique<PMA_Montes>();
    });
    REGISTER_PMA("pma_menghani1", "Implementation #1 by Gaurav Menghani, from GitHub. Not working: it hits a segmentation fault somewhere.", [](){
        return make_unique<PMA_Menghani_1>();
    });
    REGISTER_PMA("pma_menghani2", "Implementation #2 by Gaurav Menghani, from GitHub", [](){
        return make_unique<PMA_Menghani_2>();
    });
    REGISTER_PMA("pma_sha", "CPU baseline used in the GPMA paper, by Mo Sha. Extracted from GitHub: https://github.com/desert0616/gpma_bfs_demo", [](){
        return make_unique<pma::sha::PMA>();
    });
    REGISTER_PMA("pma_khayyat", "Adapted from the source code of the IEJoin paper, by Z. Khayyat et al. However, their PMA implementation is based on the source code of G. Menghani. ", [](){
        return make_unique<pma::khayyat::PackedMemoryArray>();
    });
#if defined(HAVE_PMA_DFR)
    REGISTER_PMA("pma_dfr", "Adapted from the source code of the DFR paper, provided by B. Raffin 09/05/2018", [](){
        // We ignore the internal settings of the thresholds. While they can be customised, the point of this
        // experiment is just to check the raw insertion & scan rate of their implementation.
        return make_unique<pma::dfr::PackedMemoryArray>();
    });
#endif

    REGISTER_PMA("pma_v4", "Basic packed memory array, v4", [](){
       return make_unique<PMA_Impl4>();
    });
    REGISTER_PMA("btree_v2", "Dynamic AB-tree, version 2", [](){
        auto iB = ARGREF(uint64_t, "inode_block_size").get();
        auto iA = iB / 2;
        auto lB = ARGREF(uint64_t, "leaf_block_size").get();
        auto lA = lB / 2;
        LOG_VERBOSE("[ABTree] iA=" << iA << ", iB=" << iB << ", lA=" << lA << ", lB=" << lB);

        auto btree = make_unique<abtree::ABTree>(iA, iB, lA, lB);

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        btree->set_record_leaf_statistics(record_leaf_statistics);

        return btree;
    });

    REGISTER_PMA("art", "it replaces the inner nodes of an (a,b)-tree with an ART index.", [](){
        auto iB = ARGREF(uint64_t, "inode_block_size").get();
        auto lB = ARGREF(uint64_t, "leaf_block_size").get();
        LOG_VERBOSE("[ART] Parameter inode_block_size ignored: " << iB);
        LOG_VERBOSE("[ART] block size: " << lB);

        auto art = make_unique<abtree::ART>(lB);

        return art;
    });

    REGISTER_PMA("dense_array", "Static dense arrays. It can be used in conjunction with huge pages.", [](){
        auto iB = ARGREF(uint64_t, "inode_block_size").get();
        LOG_VERBOSE("[dense_array] Parameter inode_block_size ignored: " << iB);
        auto lB = ARGREF(uint64_t, "leaf_block_size").get();
        LOG_VERBOSE("[dense_array] block size: " << lB << ", huge pages: " << (configuration::use_huge_pages() ? "true" : "false"));
        return make_unique<abtree::DenseArray>(lB);
    });

    PARAMETER(bool, "abtree_random_permutation")
        .descr("Randomly permute the nodes in the tree. Only significant for the baseline abtree implementation (btree_v2).");
    PARAMETER(bool, "record_leaf_statistics")
        .descr("When deleting the index, record in the table `btree_leaf_statistics' the statistics related to the memory distance among consecutive leaves/segments. Supported only by the algorithms btree_v2, btreecc_pma4 and apma_clocked");

    REGISTER_PMA("btree_pma_v2", "Enhanced version of btree_pma.", [](){
        auto index_B = ARGREF(uint64_t, "iB");
        auto storage_B = ARGREF(uint64_t, "lB");
        LOG_VERBOSE("[BTREE/PMA v2] index block size: " << index_B << ", storage block size: " << storage_B);
        return make_unique<BTreePMA_v2>(index_B, storage_B);
    });

    REGISTER_PMA("btree_pma_v4a", "Clustered elements, dynamic index, split key/values, non fixed sized of the segments", [](){
        auto index_B = ARGREF(uint64_t, "iB");
        auto leaf_B = ARGREF(uint64_t, "lB");
        LOG_VERBOSE("[BTREE/PMA v4a] Parameters ignored: iB: " << index_B << ", lB: " << leaf_B);
        LOG_VERBOSE("[BTREE/PMA v4a] It uses a dynamic (a,b)-tree of node size fixed to 64");
        return make_unique<BTreePMA_v4>();
    });
    REGISTER_PMA("btree_pma_v4b", "Clustered elements, dynamic index, split key/values, fixed sized of the segments.", [](){
        auto iB = ARGREF(uint64_t, "inode_block_size").get();
        auto lB = ARGREF(uint64_t, "leaf_block_size").get();
        LOG_VERBOSE("[BTREE/PMA v4b] Parameter inode_block_size ignored: " << iB);
        LOG_VERBOSE("[BTREE/PMA v4b] Segment size: " << lB);

        return make_unique<BTreePMA_v4>(lB);
    });

    REGISTER_PMA("btree_stx", "STX B+ tree, external implementation by T. Bingmaan. Note: the B+ tree parameters need to be set at compile time: make EXTRA_CXXFLAGS=\"-DSTX_BTREE_INDEX_B=<iB> -DSTX_BTREE_LEAF_B=<lB>\"", []{
            // the runtime arguments are already checked at pma::prepare_parameters();
            return make_unique<abtree::STXBtree>();
    });

    /**
     * Clustered PMA with a static AB-Tree on top
     */
    REGISTER_PMA("btreecc_pma5b", "Clustered PMA. As btreecc_pma4, plus support to remove the elements and set the density thresholds (rho/theta).",
            []{
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        LOG_VERBOSE("[btreecc_pma5b] index block size (iB): " << iB << ", segment size (lB): " << lB);
        auto algorithm = make_unique<BTreePMACC5>(iB, lB);

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        algorithm->set_record_segment_statistics(record_leaf_statistics);

        return algorithm;
    });
    REGISTER_PMA("btreecc_pma7b", "Clustered PMA with memory rewiring. Set the size of an extent with the option --extent_size=N",
            []{
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[btreecc_pma7] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        LOG_VERBOSE("[btreecc_pma7b] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes)");
        auto algorithm = make_unique<BTreePMACC7>(iB, lB, extent_mult);

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        algorithm->set_record_segment_statistics(record_leaf_statistics);

        return algorithm;
    });
    REGISTER_PMA("btreecc_pma8", "Clustered PMA with memory rewiring + Katriel's densities. Set the size of an extent with the option --extent_size=N",
            []{
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[btreecc_pma8] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        LOG_VERBOSE("[btreecc_pma8] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes)");
        auto algorithm = make_unique<v8::PackedMemoryArray8>(iB, lB, extent_mult);

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        if(record_leaf_statistics){ std::cerr << "[btreecc_pma8] Warning: parameter --record_leaf_statistics ignored" << endl; }

        return algorithm;
    });


    PARAMETER(double, "apma_predictor_scale").descr("The scale parameter to re-adjust the capacity of the predictor").set_default(1.0);

    REGISTER_PMA("apma_baseline", "Adaptive PMA, based on Hu's paper, paired with the dynamic AB-Tree of btree_pma_v2.", [](){
        auto index_B = ARGREF(uint64_t, "iB");
        auto storage_B = ARGREF(uint64_t, "lB");
        double predictor_scale = ARGREF(double, "apma_predictor_scale");
        LOG_VERBOSE("[apma_baseline] index block size (iB): " << index_B << ", storage block size (lB): " << storage_B << ", predictor_scale: " << predictor_scale);
        return make_unique<APMA_Baseline>(index_B, storage_B, predictor_scale);
    });
    REGISTER_PMA("bh07_v2b", "Adaptive PMA, based on the paper by Bender et Hu, and tuned as apma_int2. It exploits memory rewiring, set the size of an extent with the option --extent_size=N", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        double predictor_scale = ARGREF(double, "apma_predictor_scale");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[bh07_v2b] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        LOG_VERBOSE("[bh07_v2b] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes), predictor scale: " << predictor_scale);

        auto algorithm = make_unique<adaptive::bh07_v2::APMA_BH07_v2>(iB, lB, extent_mult, predictor_scale);

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        algorithm->set_record_segment_statistics(record_leaf_statistics);

        return algorithm;
    });

    { // APMA knobs
        adaptive::int1::Knobs knobs;

        auto param_rank = PARAMETER(double, "apma_rank").hint().descr("(Only for `apma_int'[1|2]) Explicitly set the rank "
                "threshold, in [0, 1], for marking segments as hammered. For instance, a value of `0.9', entails that a "
                "segment must have a minimum rank of at least 90% higher than the others to be marked as hammered");
        param_rank.set_default(knobs.get_rank_threshold());
        param_rank.validate_fn([](double value){ return (value >= 0. && value <= 1.); });

        auto param_sampling_rate = PARAMETER(double, "apma_sampling_rate").hint().descr("(Only for `apma_int'[1|2]) Sample threshold "
                "to forward an update to the detector, in [0, 1]. The value 1 implies all updates are recorded, 0 while no "
                "updates are recorded.");
        param_sampling_rate.set_default(knobs.get_sampling_rate());
        param_sampling_rate.validate_fn([](double value){ return (value >= 0. && value <= 1.); });
    }

    REGISTER_PMA("apma_int1", "Adaptive PMA with intervals.", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        LOG_VERBOSE("[apma_int1] index block size (iB): " << iB << ", segment size (lB): " << lB);
        auto algorithm = make_unique<adaptive::int1::PackedMemoryArray>(iB, lB);

        // Rank threshold
        auto argument_rank = ARGREF(double, "apma_rank");
        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }

        // Sample threshold
        auto apma_sample_rate = ARGREF(double, "apma_sampling_rate");
        if(apma_sample_rate.is_set()) { algorithm->knobs().set_sampling_rate(apma_sample_rate.get()); }

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        algorithm->set_record_segment_statistics(record_leaf_statistics);

        return algorithm;
    });
    REGISTER_PMA("apma_int2b", "Adaptive PMA with memory rewiring. Set the size of an extent with the option --extent_size=N", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[apma_int2] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        LOG_VERBOSE("[apma_int2b] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes)");
        auto algorithm = make_unique<adaptive::int2::PackedMemoryArray>(iB, lB, extent_mult);

        // Rank threshold
        auto argument_rank = ARGREF(double, "apma_rank");
        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }

        // Sample threshold
        auto apma_sample_rate = ARGREF(double, "apma_sampling_rate");
        if(apma_sample_rate.is_set()) { algorithm->knobs().set_sampling_rate(apma_sample_rate.get()); }

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        algorithm->set_record_segment_statistics(record_leaf_statistics);

        return algorithm;
    });

    REGISTER_PMA("apma_int3", "Adaptive PMA with memory rewiring & Katriel's thresholds. Set the size of an extent with the option --extent_size=N", [](){
        uint64_t iB = ARGREF(uint64_t, "iB");
        uint64_t lB = ARGREF(uint64_t, "lB");
        auto param_extent_mult = ARGREF(uint64_t, "extent_size");
        if(!param_extent_mult.is_set())
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "[apma_int3] Mandatory parameter --extent size not set.");
        uint64_t extent_mult = param_extent_mult.get();
        LOG_VERBOSE("[apma_int3] index block size (iB): " << iB << ", segment size (lB): " << lB << ", "
                "extent size: " << extent_mult << " (" << get_memory_page_size() * extent_mult << " bytes)");
        auto algorithm = make_unique<adaptive::int3::PackedMemoryArray>(iB, lB, extent_mult);

        // Rank threshold
        auto argument_rank = ARGREF(double, "apma_rank");
        if(argument_rank.is_set()){ algorithm->knobs().m_rank_threshold = argument_rank.get(); }

        // Record leaf statistics?
        bool record_leaf_statistics { false };
        ARGREF(bool, "record_leaf_statistics").get(record_leaf_statistics);
        algorithm->set_record_segment_statistics(record_leaf_statistics);

        return algorithm;
    });

    auto param_range_query_intervals = PARAMETER(string, "rqint").hint()
            .descr("Explicitly set the intervals, in (0, 1], to consider the `range_query' experiment. The value must be a comma separated list, e.g. --rqint=\"0.01, 0.1, 1\"");
    /**
     * Experiment range_query
     */
    auto fetch_range_query_intervals = [](){
        auto param_range_query_intervals = ARGREF(string, "rqint");
        vector<double> intervals;

        string user_intervals_str;
        if(param_range_query_intervals.get(user_intervals_str)){
            vector<string> user_intervals = split(user_intervals_str);
            for(decltype(auto) user_interval : user_intervals){
                size_t idx = 0;
                double interval = std::stod(user_interval, &idx);
                if(interval <= 0 || interval > 1 || idx != user_interval.size()){
                    RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid interval: `" << user_interval << "'" <<
                            " for the argument --rqint: " << user_intervals_str << ". Expected a comma separated list of numbers in (0, 1].");
                }
                intervals.push_back(interval);
            }
        }

        return intervals;
    };

    REGISTER_EXPERIMENT("range_query", "Perform multiple range queries over different intervals", [&fetch_range_query_intervals](shared_ptr<Interface> pma){
        vector<double> intervals = fetch_range_query_intervals();
        return make_unique<ExperimentRangeQueryIntervals>(pma, ARGREF(int64_t, "num_inserts"), ARGREF(int64_t, "num_lookups"), intervals);
    });

    /**
     * Experiment step_insert_lookup
     */
    REGISTER_EXPERIMENT("step_insert_lookup", "Insert the elements gradually, by doubling the size of the data structure, and measure at each step both the insert & search time",
            [](shared_ptr<Interface> interface){
        auto N_inserts = ARGREF(int64_t, "I");
        auto N_lookups = ARGREF(int64_t, "L");
        return make_unique<ExperimentStepInsertLookup>(interface, N_inserts, N_lookups);
    });

    /**
     * Experiment insert_lookup
     */
    REGISTER_EXPERIMENT("insert_lookup", "Measure the time insert `num_insertions' elements in the data structure. Eventually perform `num_lookups' lookups of random chosen at random.",
            [](shared_ptr<Interface> interface){
        auto N_inserts = ARGREF(int64_t, "I");
        auto N_lookups = ARGREF(int64_t, "L");
        return make_unique<ExperimentInsertLookup>(interface, N_inserts, N_lookups);
    });

    /**
     * Experiment step_insert_scan
     */
    REGISTER_EXPERIMENT("step_insert_scan", "Starting from `initial_size', insert up to `num_insertions' elements at steps of `idls_group_size'. At each step perform `num_lookups' look ups and `num_scans' complete scans.",
            [](shared_ptr<Interface> interface){
        auto initial_size = ARGREF(int64_t, "initial_size");
        auto step_size = ARGREF(int64_t, "idls_group_size");
        auto final_size = ARGREF(int64_t, "I");
        auto num_lookups = ARGREF(int64_t, "L");
        auto num_scans = ARGREF(int64_t, "S");

        LOG_VERBOSE("Experiment step_insert_scan with initial_size: " << initial_size << ", final_size: " << final_size << ", step_size: " << step_size << ", num_lookups: " << num_lookups << ", num_scans: " << num_scans);
        return make_unique<ExperimentStepInsertScan>(interface, initial_size, final_size, step_size, num_lookups, num_scans);
    });

    /**
     * IDLS experiment
     */
    REGISTER_EXPERIMENT("idls", "Perform `initial_size' insertions in the data structure at the start. Afterward perform `num_insertions' operations split in groups of `idls_group_size' consecutive inserts/deletes. Optionally perform `num_lookups' lookups and `num_scans' range scans.",
        [&fetch_range_query_intervals](shared_ptr<Interface> interface){
        auto N_initial_inserts = ARGREF(int64_t, "initial_size");
        auto N_insdel = ARGREF(int64_t, "I");
        auto N_lookups = ARGREF(int64_t, "L");
        auto N_scans = ARGREF(int64_t, "S");
        auto N_consecutive_operations = ARGREF(int64_t, "idls_group_size");

        vector<double> rq_intervals = fetch_range_query_intervals();
        auto insert_distribution = ARGREF(string, "distribution");
        auto insert_alpha = ARGREF(double, "alpha");
        auto param_delete_distribution = ARGREF(string, "idls_delete_distribution");
        auto param_delete_alpha = ARGREF(double, "idls_delete_alpha");
        string delete_distribution = param_delete_distribution.is_set() ? param_delete_distribution.get() : insert_distribution.get();
        double delete_alpha = param_delete_alpha.is_set() ? param_delete_alpha.get() : insert_alpha.get();
        auto beta = ARGREF(double, "beta");
        auto seed = ARGREF(uint64_t, "seed_random_permutation");

        LOG_VERBOSE("idls, inserts: " << insert_distribution.get() << " (" << insert_alpha.get() << "), deletes: " << delete_distribution << " (" << delete_alpha << "), range: " << static_cast<int64_t>(beta.get()));

        return make_unique<ExperimentIDLS>(interface, N_initial_inserts, N_insdel, N_consecutive_operations,
                N_lookups, N_scans, rq_intervals,

                insert_distribution, insert_alpha,
                delete_distribution, delete_alpha,
                beta, seed);
    });

    REGISTER_EXPERIMENT("bandwidth_idls", "Perform `initial_size' insertions in the data structure at the start. Afterward perform `num_insertions' operations split in groups of `idls_group_size' consecutive inserts/deletes. Record the bandwidth each second.",
        [](shared_ptr<Interface> interface){
        auto N_initial_inserts = ARGREF(int64_t, "initial_size");
        auto N_insdel = ARGREF(int64_t, "I");
        auto N_lookups = ARGREF(int64_t, "L");
        if(N_lookups != 0) std::cerr << "[WARNING] Argument -L (--num_lookups) ignored in this experiment" << std::endl;
        auto N_scans = ARGREF(int64_t, "S");
        if(N_scans != 0) std::cerr << "[WARNING] Argument -S (--num_scans) ignored in this experiment" << std::endl;

        auto N_consecutive_operations = ARGREF(int64_t, "idls_group_size");

        auto insert_distribution = ARGREF(string, "distribution");
        auto insert_alpha = ARGREF(double, "alpha");
        auto param_delete_distribution = ARGREF(string, "idls_delete_distribution");
        auto param_delete_alpha = ARGREF(double, "idls_delete_alpha");
        string delete_distribution = param_delete_distribution.is_set() ? param_delete_distribution.get() : insert_distribution.get();
        double delete_alpha = param_delete_alpha.is_set() ? param_delete_alpha.get() : insert_alpha.get();
        auto beta = ARGREF(double, "beta");
        auto seed = ARGREF(uint64_t, "seed_random_permutation");

        LOG_VERBOSE("bandwidth_idls, initial insertions: " << N_initial_inserts << ", updates: " << N_insdel << ", group size: " << N_consecutive_operations);
        LOG_VERBOSE("bandwidth_idls, distribution inserts: " << insert_distribution.get() << " (" << insert_alpha.get() << "), deletes: " << delete_distribution << " (" << delete_alpha << "), range: " << static_cast<int64_t>(beta.get()));

        return make_unique<ExperimentBandwidthIDLS>(interface, N_initial_inserts, N_insdel, N_consecutive_operations,
                insert_distribution, insert_alpha,
                delete_distribution, delete_alpha,
                beta, seed);
    });

    /**
     * Experiment step_idls
     */
    REGISTER_EXPERIMENT("step_idls", "Starting from `initial_size', first insert up to `num_insertions' elements. Afterward, delete all the elements inserted. Perform both inserts and deletes at steps of `idls_group_size'. At each step, run `num_scans' complete scans.",
            [](shared_ptr<Interface> interface){
        auto initial_size = ARGREF(int64_t, "initial_size");
        auto step_size = ARGREF(int64_t, "idls_group_size");
        auto final_size = ARGREF(int64_t, "I");
        auto num_scans = ARGREF(int64_t, "S");

        auto insert_distribution = ARGREF(string, "distribution");
        auto insert_alpha = ARGREF(double, "alpha");
        auto param_delete_distribution = ARGREF(string, "idls_delete_distribution");
        auto param_delete_alpha = ARGREF(double, "idls_delete_alpha");
        string delete_distribution = param_delete_distribution.is_set() ? param_delete_distribution.get() : insert_distribution.get();
        double delete_alpha = param_delete_alpha.is_set() ? param_delete_alpha.get() : insert_alpha.get();
        auto beta = ARGREF(double, "beta");
        auto seed = ARGREF(uint64_t, "seed_random_permutation");

        LOG_VERBOSE("Experiment step_idls with initial_size: " << initial_size << ", final_size: " << final_size << ", step_size: " << step_size << ", num_scans: " << num_scans);
        LOG_VERBOSE("step_idls, distribution inserts: " << insert_distribution.get() << " (" << insert_alpha.get() << "), deletes: " << delete_distribution << " (" << delete_alpha << "), range: " << static_cast<int64_t>(beta.get()));

        return make_unique<ExperimentStepIDLS>(interface, initial_size, final_size, step_size, num_scans,
                insert_distribution, insert_alpha, delete_distribution, delete_alpha, beta, seed);
    });


    /**
     * Aging experiment
     */
    PARAMETER(uint64_t, "scan_warmup").hint("N").descr("Perform `N' non recorded warm-up scan iterations before starting the experiment").set_default(0);
    PARAMETER(string, "temp").hint("path").descr("Path to a temporary folder, for disk spilling.").set_default("/tmp");
    REGISTER_EXPERIMENT("aging", "Similar to the IDLS experiment. Perform batches of insertions/deletions followed by full scans. The data structure to test has to be an ab-tree.",
        [](shared_ptr<Interface> interface){
        auto N_initial_inserts = ARGREF(int64_t, "initial_size");
        auto N_insdel = ARGREF(int64_t, "I");
        auto N_warmup = ARGREF(uint64_t, "scan_warmup");
        auto N_scans = ARGREF(int64_t, "S");
        auto N_consecutive_operations = ARGREF(int64_t, "idls_group_size");

        string distribution = ARGREF(string, "distribution");
        if(distribution != "uniform"){
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid distribution: `" << distribution << "'. For the `aging' experiment, only the `uniform' distribution is currently supported.");
        }

        auto seed = ARGREF(uint64_t, "seed_random_permutation");
        auto tmpfolder = ARGREF(string, "temp");

        LOG_VERBOSE("aging, initial size: " << N_initial_inserts << ", total operations: " << N_insdel << ", consecutive operations (inserts/deletes): " << N_consecutive_operations << ", scans: " << N_scans);
        return make_unique<ExperimentAging>(interface, N_initial_inserts, N_insdel, N_consecutive_operations, N_warmup, N_scans, tmpfolder, seed);
    });

    /**
     * BulkLoading experiment
     */
    PARAMETER(int64_t, "batch_size").hint()
            .validate_fn([](int64_t value){ return value >= 1; })
            .descr("The size of each individual batch. Only valid for the experiment `bulk_loading'.");
    PARAMETER(int64_t, "num_batches").hint()
            .validate_fn([](int64_t value){ return value >= 1; })
            .descr("The number of batches to load. Only valid for the experiment `bulk_loading'.");
    PARAMETER(bool, "initial_size_uniform").descr("Whether to load the first `initial_size' elements with a uniform distribution");
    REGISTER_EXPERIMENT("bulk_loading", "Load the data structure in `batches'. It requires the parameters `batch_size' and 'num_batches' to be explicitly set. Sample usage: ./pma_comp ... -e bulk_loading --batch_size 1024 --num_batches 8",
    [](shared_ptr<Interface> interface){
        // initial size
        uint64_t initial_size = 0;
        auto arg_initial_size = ARGREF(int64_t, "initial_size");
        if(arg_initial_size.is_set()){
            if(arg_initial_size.get() < 0){
                RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid initial size < 0: `" << arg_initial_size.get() << "'");
            }
            initial_size = arg_initial_size.get();
        }

        // the size of each batch
        uint64_t batch_size = 0;
        auto arg_batch_size = ARGREF(int64_t, "batch_size"); // value >= 1, already validated
        if(!arg_batch_size.is_set()){
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Required parameter batch_size not set. Sample usage: ./pma_comp ... -e bulk_loading --batch_size 1024 --num_batches 8");
        }
        batch_size = arg_batch_size.get();

        // the number of batches to perform
        uint64_t num_batches = 0;
        auto arg_num_batches = ARGREF(int64_t, "num_batches"); // value >= 1, already validated
        if(!arg_num_batches.is_set()){
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Required parameter num_batches not set. Sample usage: ./pma_comp ... -e bulk_loading --batch_size 1024 --num_batches 8");
        }
        num_batches = arg_num_batches.get();

        // load the first elements with a uniform distribution ?
        bool is_initial_size_uniform = false;
        auto arg_initial_size_uniform = ARGREF(bool, "initial_size_uniform");
        if(arg_initial_size_uniform.is_set() && arg_initial_size_uniform.get()){
            is_initial_size_uniform = true;
        }

        LOG_VERBOSE("bulk loading, initial size: " << initial_size << ", batch size: " << batch_size << ", number of batches: " << num_batches << ", init uniform: " << boolalpha << is_initial_size_uniform);
        return make_unique<ExperimentBulkLoading>(interface, initial_size, batch_size, num_batches, is_initial_size_uniform);
    });



    { // the list of available algorithms
        stringstream helpstr;
        helpstr << "The algorithm to evaluate. The possible choices are:";
        for(size_t i = 0; i < factory().algorithms().size(); i++){
            auto& item = factory().algorithms()[i];
            helpstr << "\n- " << item->name() << ": " << item->description();
        }

        PARAMETER(string, "algorithm")["a"].hint().required().record(false)
                .descr(helpstr.str())
                .validate_fn([](const std::string& algorithm){
            auto& list = factory().algorithms();
            auto res = find_if(begin(list), end(list), [&algorithm](auto& impl){
                return impl->name() == algorithm;
            });
            if(res == end(list))
                RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid algorithm: " << algorithm);
            return true;
        });
    }

    PARAMETER(uint64_t, "inode_block_size")["b"].hint().set_default(64)
                        .descr("The block size for the intermediate nodes");
    PARAMETER(uint64_t, "leaf_block_size")["l"].hint().set_default(128)
                        .descr("The block size of the leaves");

    // IDLS experiment
    PARAMETER(int64_t, "idls_group_size").hint("N >= 1").set_default(1)
            .descr("Size of consecutive inserts/deletes in the IDLS experiment.")
            .validate_fn([](int64_t value){ return value >= 1; });
    PARAMETER(string, "idls_delete_distribution")
            .descr("The distribution for the deletions in the IDLS experiment. By default it's the same as inserts. Valid values are `uniform' and `zipf'.");
    PARAMETER(double, "idls_delete_alpha")
            .descr("Rho factor in case the delete distribution is Zipf");

    // Density constraints
    PARAMETER(double, "rho_0").hint().set_default(0.08)
            .descr("Lower density in the PMA for the lowest level of the calibrator tree, i.e. the segments.");
    PARAMETER(double, "rho_h").hint().set_default(0.3)
            .descr("Lower density in the PMA for the highest level of the calibrator tree, i.e. the root.");
    PARAMETER(double, "theta_h").hint().set_default(0.75)
            .descr("Upper density in the PMA for the highest level of the calibrator tree, i.e. the root");
    PARAMETER(double, "theta_0").hint().set_default(1.0)
            .descr("Upper density in the PMA for the lowest level of the calibrator tree, i.e. the segments");

    { // the list of experiments
        stringstream helpstr;
        helpstr << "The experiment to perform. The possible choices are: ";
        for(size_t i = 0; i < factory().experiments().size(); i++){
            auto& e = factory().experiments()[i];
            if(!e->is_display()) continue;
            helpstr << "\n- " << e->name() << ": " << e->description();
        }

        PARAMETER(string, "experiment")["e"].hint().required().record(false)
                .descr(helpstr.str())
                .validate_fn([](const std::string& experiment){
            auto& list = factory().experiments();
            auto res = find_if(begin(list), end(list), [&experiment](auto& impl){
                return impl->name() == experiment;
            });
            if(res == end(list))
                RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Invalid experiment: " << experiment);
            return true;
        });
    }

    initialised = true;
}


void execute(){
    if(!initialised) RAISE_EXCEPTION(Exception, "pma::initialise() has not been called");

    string name_algorithm = ARGREF(string, "algorithm");
    string name_experiment = ARGREF(string, "experiment");
    shared_ptr<Experiment> experiment;

    // standard single-payload scenario
    experiment = factory().make_experiment(name_experiment, factory().make_algorithm(name_algorithm));
    experiment->execute();
}


// Unfortunately, the B+ Tree STX requires the tree parameters to be set at compile time.
// This block checks that the parameters set at run-time are the same of those set at compile time
static void prepare_parameters_btree_stx() {
    abtree::STXBtree::btree_traits traits;

    auto lB = PARAMETER(uint64_t, "lB");
    auto iB = PARAMETER(uint64_t, "iB");

    // leaves
    if(lB.is_default() || !lB.is_set()){
        if(traits.leafslots != lB){
            lB.set_forced(traits.leafslots);
        }
    } else { // explicitly set
        if(traits.leafslots != lB){ // sorry!
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Algorithm STX B+ Tree. Parameter -l set to: " << lB.get() <<
                    ", but the compile time macro STX_BTREE_LEAF_B=" << traits.leafslots << ". Use "
                    "`make EXTRA_CXXFLAGS=\"-DSTX_BTREE_INDEX_B="<< iB << " -DSTX_BTREE_LEAF_B=" << lB << "\"' to set "
                    "the tree properties at compile time.");
        }
    }

    // same thing for the inodes
    if(iB.is_default() || !iB.is_set()){
        if(traits.innerslots != iB){
            iB.set_forced(traits.innerslots);
        }
    } else { // explicitly set
        if(traits.innerslots != iB){ // sorry!
            RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Algorithm STX B+ Tree. Parameter -b set to: " << iB.get() <<
                    ", but the compile time macro STX_BTREE_INDEX_B=" << traits.innerslots << ". Use "
                    "`make EXTRA_CXXFLAGS=\"-DSTX_BTREE_INDEX_B="<< iB << " -DSTX_BTREE_LEAF_B=" << lB << "\"' to set "
                    "the tree properties at compile time.");
        }
    }

    LOG_VERBOSE("[B+Tree STX] iB=" << iB << ", lB=" << lB);
}

/**
 * For the experiment `bulk_loading' the number of insertions is effectively ignored. However we need to set
 * to properly initialise the distribution to use.
 */
static void prepare_parameters_bulk_loading(){
    // initial size
    uint64_t initial_size = ARGREF(int64_t, "initial_size");

    // the size of each batch
    uint64_t batch_size = 0;
    auto arg_batch_size = ARGREF(int64_t, "batch_size"); // value >= 1, already validated
    if(!arg_batch_size.is_set()){
        RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Required parameter batch_size not set. Sample usage: ./pma_comp ... -e bulk_loading --batch_size 1024 --num_batches 8");
    }
    batch_size = arg_batch_size.get();

    // the number of batches to perform
    uint64_t num_batches = 0;
    auto arg_num_batches = ARGREF(int64_t, "num_batches"); // value >= 1, already validated
    if(!arg_num_batches.is_set()){
        RAISE_EXCEPTION(configuration::ConsoleArgumentError, "Required parameter num_batches not set. Sample usage: ./pma_comp ... -e bulk_loading --batch_size 1024 --num_batches 8");
    }
    num_batches = arg_num_batches.get();

    size_t num_inserts = initial_size + batch_size * num_batches;
    auto arg_num_inserts = PARAMETER(int64_t, "num_inserts");
    if(arg_num_inserts.is_set()){
        cout << "[WARNING] Overwriting the parameter `num_inserts' (-I) to " << num_inserts << " for the experiment `bulk_loading'" << endl;
    }
    arg_num_inserts.set_forced(num_inserts);
}

void prepare_parameters() {
    string algorithm = ARGREF(string, "algorithm");
    string experiment = ARGREF(string, "experiment");

    if(algorithm == "btree_stx")
        prepare_parameters_btree_stx();

    if(experiment == "bulk_loading")
        prepare_parameters_bulk_loading();

}

} // namespace pma


