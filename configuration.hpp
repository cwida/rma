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

#ifndef CONFIGURATION_HPP_
#define CONFIGURATION_HPP_

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "errorhandling.hpp"

#define LOG_VERBOSE(msg) if(config().verbose()){ std::cout << msg << std::endl; }

// forward declarations
namespace configuration {
class Configuration;
namespace details {
    class ParameterBase;
    class ParameterBaseImpl;
    template<typename T> class ParameterImpl;
    template<typename T> class TypedParameter;
} // namespace details
} // namespace configuration
namespace database {
class Database;
}

inline configuration::Configuration& config();

namespace configuration {

DEFINE_EXCEPTION(ConfigurationException);

class Configuration {
private:
    Configuration(const Configuration& ) = delete;
    Configuration& operator=(const Configuration&) = delete;

    // Singleton
    static Configuration singleton;
    friend Configuration& ::config();

    // Console arguments
    friend class details::ParameterBase;
    std::vector<details::ParameterBaseImpl*> m_console_parameters;

    // The actual instance to the database
    friend class database::Database;
    std::unique_ptr<database::Database> m_database;

    // Verbose?
    details::ParameterImpl<bool>* m_verbose = nullptr;

    // Initialise the singleton instance
    Configuration();

public:
    ~Configuration();

    /**
     * If set, the program should print extra messages to the standard output
     */
    bool verbose() const;

    void parse_command_line_args(int argc, char* argv[]);


    /**
     * Initialise the database instance
     */
    void initialise_database();

    /**
     * Retrieve the current instance of the database driver
     */
    database::Database* db(){ return m_database.get(); }

};

/**
 * Use huge pages?
 */
bool use_huge_pages();

} // namespace configuration


inline configuration::Configuration& config() {
    return configuration::Configuration::singleton;
}


#endif /* CONFIGURATION_HPP_ */
