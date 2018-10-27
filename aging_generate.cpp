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

#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "configuration.hpp"
#include "console_arguments.hpp"
#include "errorhandling.hpp"
#include "abtree/abtree.hpp"
#include "distribution/driver.hpp"
#include "distribution/sparse_uniform_distribution.hpp"
#include "pma/driver.hpp"

using namespace abtree;
using namespace std;

int main(int argc, char* argv[]){
    try {
        auto param_file_initial = PARAMETER(string, "file_initial");
        auto param_file_insert = PARAMETER(string, "file_insert");
        auto param_file_delete = PARAMETER(string, "file_delete");
        auto param_group_size = PARAMETER(int64_t, "idls_group_size").validate_fn([](int64_t value){ return value > 0; });

        // parse the command line and retrieve the user arguments
        config().parse_command_line_args(argc, argv);

        if(!param_file_initial.is_set())
            RAISE_EXCEPTION(configuration::ConfigurationException, "Argument `file_initial' not set");
        if(!param_file_insert.is_set())
            RAISE_EXCEPTION(configuration::ConfigurationException, "Argument `file_insert' not set");
        if(!param_file_delete.is_set())
            RAISE_EXCEPTION(configuration::ConfigurationException, "Argument `file_delete' not set");
        string file_initial_str = param_file_initial.get();
        string file_insert_str = param_file_insert.get();
        string file_delete_str = param_file_delete.get();


        auto arg_initial_size = ARGREF(int64_t, "initial_size");
        size_t initial_size = 0;
        if(arg_initial_size.is_set()){ initial_size = arg_initial_size.get(); }
        auto arg_total_operations = ARGREF(int64_t, "I");
        size_t total_operations = arg_total_operations.get();
        if(!param_group_size.is_set()){
            RAISE_EXCEPTION(configuration::ConfigurationException, "Parameter `idls_group_size' not set");
        }
        size_t num_consecutive_operations = param_group_size.get();

        // Distribution
        uint64_t seed = PARAMETER(uint64_t, "seed_random_permutation");
        mt19937_64 random_generator(seed);
        uniform_int_distribution<int64_t> distribution(1, (1ull << 63) -1);

        abtree::ABTree tree{64};

        if(initial_size > 0){
            LOG_VERBOSE("[aging_generate] Initial size: " << initial_size << ", output file: " << file_initial_str);
            fstream finit(file_initial_str, ios::out);
            if(!finit.good()) RAISE_EXCEPTION(configuration::ConfigurationException, "Cannot open the file: " << file_initial_str);

            for(int i = 0; i < initial_size; i++){
                uint64_t key = distribution(random_generator);
                tree.insert(key, key);
                finit.write(reinterpret_cast<const char *>(&key), sizeof(key));
            }

            finit.close();
            LOG_VERBOSE("[aging_generate] Initial load done.");
        }

        fstream finsert(file_insert_str, ios::out);
        fstream fdelete(file_delete_str, ios::out);

        LOG_VERBOSE("[aging_generate] Output file for the insertions: " << file_insert_str);
        LOG_VERBOSE("[aging_generate] Output file for the deletions: " << file_delete_str);

        size_t count_operations = 0;
        while(count_operations < total_operations){

            // Add `num_consecutive_operations' values
            for(size_t i = 0; i < num_consecutive_operations; i ++){
                uint64_t key = distribution(random_generator);
                tree.insert(key, key);
                finsert.write(reinterpret_cast<const char *>(&key), sizeof(key));
            }

            { // Remove `num_consecutive_operations' values
                size_t i = 0;
                while(i < num_consecutive_operations && tree.size() > 0){
                    uint64_t candidate_key = distribution(random_generator);
                    auto it = tree.find(candidate_key, std::numeric_limits<int64_t>::max());
                    if(it->hasNext()){
                        int64_t key = it->next().first;
#if !defined(NDEBUG) // sanity check
                        assert(tree.find(key) == key);
                        size_t sz_before = tree.size();
#endif

                        tree.remove(key);
                        fdelete.write(reinterpret_cast<const char *>(&key), sizeof(key));

#if !defined(NDEBUG) // sanity check
                        size_t sz_after = tree.size();
                        assert(sz_after +1 == sz_before);
#endif
                        i++;
                    }
                }
            }



            assert(tree.size() == initial_size && "The (a,b)-tree should have the same cardinality as the start");
            count_operations += 2 * num_consecutive_operations;
        }


        finsert.close();
        fdelete.close();

        cout << "[aging_generate] Done\n" << endl;
    } catch (const Exception& e){
        cerr << "Kind: " << e.getExceptionClass() << ", file: " << e.getFile() << ", function: " << e.getFunction() << ", line: " << e.getLine() << "\n";
        cerr << "ERROR: " << e.what() << "\n";
    }

    return 0;
}
