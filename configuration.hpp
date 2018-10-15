/*
 * configuration.hpp
 *
 *  Created on: 24 Jul 2017
 *      Author: Dean De Leo
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
