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

#include <iostream>

#include "configuration.hpp"
#include "database.hpp"
#include "errorhandling.hpp"
#include "distribution/driver.hpp"
#include "pma/driver.hpp"

using namespace std;

int main(int argc, char* argv[]){
    try {
        // initialise the available data structures and experiments that can be executed
        pma::initialise();

        // initialise the distributions
        distribution::initialise();

        // parse the command line and retrieve the user arguments
        config().parse_command_line_args(argc, argv);

        // manipulate the user arguments before storing into the database, possibly perform some sanity checks
        pma::prepare_parameters();

        // store the current settings, environment & parameters into a SQLite database
        config().initialise_database();

        // create the required data structure & fire the experiment
        pma::execute();

        cout << "Done\n" << endl;
    } catch (const Exception& e){
        cerr << "Kind: " << e.getExceptionClass() << ", file: " << e.getFile() << ", function: " << e.getFunction() << ", line: " << e.getLine() << "\n";
        cerr << "ERROR: " << e.what() << "\n";
    }

    return 0;
}
